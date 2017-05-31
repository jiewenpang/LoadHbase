package com.asiainfo.loadhbase.handler;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.asiainfo.loadhbase.MainApp;
import com.asiainfo.loadhbase.resource.Record;
import com.asiainfo.loadhbase.tool.FtpTools;
import com.asiainfo.loadhbase.tool.LCompress;

public class MRHander extends BaseHandler {

	protected String name;
	protected int tdnum;
	protected String input;
	protected String inputlarge;
	protected String fileNamePrefix;
	// Mapper每台机子的运行线程数，最好是CPU逻辑单元-1，但是也要考虑FTP连接容量
	protected int maptasks;
	protected String userName;
	protected boolean distributeMode;
	// 1表示备份，0表示不备份
	protected String isbakinput;
	// -1表示无需出明细文件
	protected String detailoutputdir;
	protected String bakdir;
	protected String largesize;

	@Override
	public void run() throws Exception {

		List<FileInfo> fileInfoList = new ArrayList<FileInfo>();
		Long totalsize = GetEveryFiles(fileInfoList);
		int filenum = fileInfoList.size();
		Long avgfilesize = totalsize / maptotalnum; // 每个map处理的文件总大小
		if (filenum == 0) {
			logger.info("task finish,quit!");
			return;
		}
		logger.info("ftp list finish:" + filenum + ",avgfilesize:" + avgfilesize + "");
		Path inputpath = new Path(input);
		FileSystem fs = FileSystem.get(hbaseConfiguration);

		if (!fs.exists(inputpath)) {
			fs.mkdirs(inputpath);
			fs.mkdirs(inputpath, new FsPermission("777"));
		} else {
			logger.info("input file existed!clean...");
			fs.delete(inputpath, true);
			logger.info("clean finish...create folder");
			fs.mkdirs(inputpath);
			fs.mkdirs(inputpath, new FsPermission("777"));
		}

		InputHDFS(tdnum, input, fs, fileInfoList, avgfilesize, 2);

		logger.info("config Job begin..");
		JobConf conf = new JobConf(hbaseConfiguration);
		conf.setNumMapTasks(filenum / tdnum);
		conf.set("record", recordClassName);
		conf.set("region", region);
		conf.set("family", tabFamily);
		conf.set("filterregion", filterregion);
		conf.set("column", column);
		conf.set("ischgport", ischgport);
		conf.set("isbakinput", isbakinput);
		conf.set("inputlarge", inputlarge);
		conf.set("largesize", largesize);
		conf.set("detailoutputdir", detailoutputdir);
		conf.set("bakdir", bakdir);
		conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 150 * tdnum);
		conf.setLong("mapreduce.input.fileinputformat.split.minsize", 1L);
		conf.setLong("mapred.min.split.size", 1L);
		conf.setInt("mapred.task.timeout", 3600000);
		conf.setLong("mapred.tasktracker.map.tasks.maximum", maptasks);
		// mapper失败重试次数，默认是4.在准生产上有机子挂掉,网络联通不了会导致部分失败，必须设置此参数，否则命中失败次数太多会导致整个Job失败。
		conf.setInt("mapred.max.map.failures.percent", 20);

		Job job = Job.getInstance(conf);
		job.setJobName("QJDBASE_JOB_" + fileNamePrefix + new SimpleDateFormat("yyyyMM").format(new Date()));
		job.setMapperClass(Map.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, inputpath);
		logger.info("config HbaseJOB...");

		long sttime = System.currentTimeMillis();
		job.setJarByClass(MainApp.class);
		logger.info("excute Job begin..");
		TableMapReduceUtil.addDependencyJars(job);
		TableMapReduceUtil.initCredentials(job);

		logger.info((job.waitForCompletion(true) ? "excute task successfully!" : "excute task fail!"));
		logger.info("clean begin...");
		fs.delete(inputpath, true);
		long endtime = System.currentTimeMillis();
		logger.info("file list:" + filenum + ",all task finish,total time:" + (endtime - sttime) / 1000 + "s,quit...");

	}

	private static void InputHDFS(int tdnum, String input, FileSystem fs, List<FileInfo> fileinfos, Long avgfilesize,
			int itype) throws InterruptedException {
		ExecutorService pool = Executors.newFixedThreadPool(50);

		if (itype == 1) { // 按文件大小分片
			Long currentfilesize = 0l;// 当前文件总大小
			int beginindex = 0;
			int endindex = 0;
			CreatHDFSFile td = null;
			for (int i = beginindex, len = fileinfos.size(); i < len; i++) {
				currentfilesize += fileinfos.get(i).getSize();
				if (avgfilesize <= currentfilesize) {
					endindex = i + 1;
					td = new CreatHDFSFile(fs, input, fileinfos, beginindex, endindex);
					beginindex = endindex;
					currentfilesize = 0l;
					pool.execute(td);
				}
			}
			if (endindex != fileinfos.size()) {
				td = new CreatHDFSFile(fs, input, fileinfos, beginindex, fileinfos.size());
				pool.execute(td);
			}
		} else { // 按文件数量分片
			if (fileinfos.size() - tdnum < 0) {
				CreatHDFSFile td = new CreatHDFSFile(fs, input, fileinfos, 0, fileinfos.size());
				pool.execute(td);
			} else {
				for (int i = 0; i < fileinfos.size(); i += tdnum) {
					CreatHDFSFile td;
					if (i + tdnum <= fileinfos.size()) {
						td = new CreatHDFSFile(fs, input, fileinfos, i, i + tdnum);
					} else {
						td = new CreatHDFSFile(fs, input, fileinfos, i, fileinfos.size());
					}
					pool.execute(td);
				}
			}
		}
		pool.shutdown();
		while (!pool.awaitTermination(10, TimeUnit.SECONDS))
			;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		static final Log LOG = LogFactory.getLog(Map.class);
		private static final String CHARTSET = "GBK";
		private static String recordClassName;
		private static Record record;
		private static String[] regions;
		private static String[] family;
		private static String[] columns;
		private static String filterregion;
		private static String ischgport;
		private static String isbakinput;
		private static String inputlargepath;
		private static Long largesize;
		private String detail_fileName = "";
		private static String detailoutputdir;
		private static String bakdir;
		private static SimpleDateFormat sf = new SimpleDateFormat("yyyyMMddHH24mmss");

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			LOG.info("init");
			recordClassName = context.getConfiguration().get("record");
			regions = context.getConfiguration().get("region").split(",");
			family = context.getConfiguration().get("family").split(",");
			columns = context.getConfiguration().get("column").split(",");
			filterregion = context.getConfiguration().get("filterregion");
			ischgport = context.getConfiguration().get("ischgport");
			isbakinput = context.getConfiguration().get("isbakinput");
			inputlargepath = context.getConfiguration().get("inputlarge");
			largesize = Long.valueOf(context.getConfiguration().get("largesize"));
			detailoutputdir = context.getConfiguration().get("detailoutputdir");
			bakdir = context.getConfiguration().get("bakdir");
			try {
				record = (Record) Class.forName(recordClassName).newInstance();

				// 设置列族
				record.setFamilyNames(family);

				// 设置列名
				record.setColumns(columns);

				// 设置分区依据
				record.setRegions(regions);

				// 设置分区依据
				record.setFilterRegion(filterregion);

			} catch (Exception e) {
				e.printStackTrace();
				LOG.error("loadclsError!", e);
			}
			System.out.println("set conf secusse!");
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Iterator<Entry<String, FtpTools>> it = FtpTools.ftpClientList.entrySet().iterator();
			LOG.info("cleanup:" + FtpTools.ftpClientList.size());
			FtpTools ftptools = null;
			while (it.hasNext()) {
				try {
					ftptools = it.next().getValue();
					if (ftptools.getFtpClient().changeWorkingDirectory(detailoutputdir)) {
						FTPFile[] files = ftptools.getFtpClient().listFiles(detail_fileName);
						if (files.length >= 1) {
							LOG.info("rename detailfile:"
									+ detail_fileName
									+ ",result:"
									+ ftptools.rename(detail_fileName,
											detail_fileName.substring(0, detail_fileName.length() - 4)));
						}

					}
					LOG.info("ftptools:" + ftptools.toString());
					if (ftptools.isConned()) {
						ftptools.disConnect();
					}
					ftptools = null;
				} catch (Exception e) {
					ftptools = null;
					System.out.println("close ftp");
				}
			}
			FtpTools.ftpClientList.clear();

			if (!record.mapTable.isEmpty()) {
				Iterator<Entry<String, Table>> it1 = record.mapTable.entrySet().iterator();
				while (it1.hasNext()) {
					Entry<String, Table> entry = it1.next();
					Table table = entry.getValue();
					table.close(); // 是否要先flush？
				}
			}
			record.mapTable.clear();
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			LOG.info("contents is:" + value.toString());
			String[] ftpinfo = value.toString().split(":");

			int port = 21;
			if (ischgport.equals("0"))
				port = Integer.valueOf(ftpinfo[1]);

			FtpTools ftp = FtpTools.newInstance(ftpinfo[0], port, ftpinfo[2], ftpinfo[3], ftpinfo[4]);
			int linenum = 0;// 文件总行数,无文件头
			int inputlinenum = 0;// 实际入库行数
			try {
				if (ftp.connectServer()) {
					LOG.info("connect success!" + " port:" + port);
					ByteArrayInputStream bin = null;
					FSDataInputStream inStream = null;
					BufferedReader br = null;
					// 获取原文件的行数
					if (largesize < Long.valueOf(ftpinfo[6])) {
						FileSystem fs = FileSystem.get(context.getConfiguration());
						Path datapath = new Path(inputlargepath + "/" + ftpinfo[5]);
						FSDataOutputStream out = fs.create(datapath);
						ftp.download(ftpinfo[5], out);
						out.close();
						inStream = fs.open(datapath);
						br = new BufferedReader(new InputStreamReader(inStream, CHARTSET));
					} else {
						byte[] bos = ftp.download2Buf(ftpinfo[5]);
						LOG.info("download2Buf finish!");
						if (bos.length == 0) {
							LOG.info(ftpinfo[5] + "file is empty");
							return;
						}
						if (ftpinfo[5].toLowerCase().endsWith(".z")) {// 解压
							LOG.info("ftp:deCompress");
							bos = LCompress.deCompress(bos);
							LOG.info("ftp:deCompress finish");
						}
						bin = new ByteArrayInputStream(bos);
						br = new BufferedReader(new InputStreamReader(bin, CHARTSET));
					}

					long starttime = System.currentTimeMillis();
					LOG.info("start DealFile:" + ftpinfo[5]);
					// 业务处理
					linenum = record.buildRecord(ftpinfo[5], br, connection);
					long endtime = System.currentTimeMillis();
					LOG.info("insert Hbase Finish!recodeCount:" + linenum + ",Time Consuming:" + (endtime - starttime)
							+ "ms.");
					LOG.info(ftpinfo[5] + ":process" + linenum);
					recursiondelfile(context, ftp, ftpinfo, 3, linenum, inputlinenum);
				} else {
					LOG.info("ftp error!");
				}
			} catch (SocketException e) {
				LOG.error("map SocketException:" + e.getMessage());
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				LOG.error("map UnsupportedEncodingException:" + e.getMessage());
				e.printStackTrace();
			} catch (IOException e) {
				LOG.error("map IOException:" + e.getMessage());
				ExceptionController(value, e);
			} catch (Exception e) {
				ExceptionController(value, e);
			}

		}

		public void ExceptionController(Text value, Exception e) throws InterruptedException {
			LOG.error("connect ftp:" + value.toString() + ",error!");
			e.printStackTrace();
			StringBuffer sb = new StringBuffer();
			StackTraceElement[] stackArray = e.getStackTrace();
			for (int i = 0; i < stackArray.length; i++) {
				StackTraceElement element = stackArray[i];
				if (element.toString().indexOf("asiainfo") != -1)
					sb.append(element.toString() + "\n");
			}
			throw new InterruptedException("element:" + sb.toString());
		}

		// ftp 接口机备份 处理过的文件
		public void detailbak(Context context, String[] ftpinfo, FtpTools ftp, int linenum, int inputlinenum)
				throws UnknownHostException, IOException {

			// 文件名:JobId_当前处理map主机名_当前处理主机map进程号_yyyymmddhh24miss
			if ("".equals(detail_fileName)) {
				String hostname = InetAddress.getLocalHost().getHostName();
				String runtime = ManagementFactory.getRuntimeMXBean().getName();
				Integer processnum = Integer.parseInt(runtime.substring(0, runtime.indexOf("@")));
				;
				String date = sf.format(new Date());
				detail_fileName = context.getJobID().toString() + "_" + hostname + "_" + processnum + "_" + date
						+ ".tmp";
			}
			// 文件内容:主机地址|文件名|文件大小|文件记录数|
			String content = ftpinfo[0] + "|" + ftpinfo[4] + "/" + ftpinfo[5] + "|" + ftpinfo[6] + "|" + linenum + "|"
					+ inputlinenum + "\n";
			InputStream is = new ByteArrayInputStream(content.getBytes());
			boolean flag = ftp.writeFile(is, detailoutputdir, detail_fileName);
			is.close();
			if (!flag) {
				throw new IOException("write detail file isSuccess:" + flag);
			}
		}

		private void recursiondelfile(Context context, FtpTools ftp, String[] ftpinfo, int times, int linenum,
				int inputlinenum) {
			if (times <= 0) {
				LOG.error("recursiondelfile fail filename:" + ftpinfo[5]);
			} else {
				try {
					if (ftp.connectServer()) {
						if (!detailoutputdir.equals("-1")) {
							detailbak(context, ftpinfo, ftp, linenum, inputlinenum);
						}
						if ("0".equals(isbakinput)) {
							LOG.info("delete file:" + ftpinfo[5] + "," + ftp.delete(ftpinfo[5]));
						} else {
							String tofiledir = getTofilename(ftpinfo[4], bakdir);
							ftp.rename(ftpinfo[4] + "/" + ftpinfo[5], tofiledir + "/" + ftpinfo[5]);
						}
					} else {
						LOG.warn("ftp login fail!");
					}
				} catch (IOException e) {
					times = times - 1;
					LOG.warn("recursiondelfile IOException:" + e.getMessage());
					e.printStackTrace();
					recursiondelfile(context, ftp, ftpinfo, times, linenum, inputlinenum);
				}
			}
		}

		private String getTofilename(String dir, String bakdir) {
			int idx = dir.lastIndexOf("/");
			if (dir.length() == idx + 1)
				idx = dir.lastIndexOf("/", idx - 1);

			String tofiledir = dir.substring(0, idx) + "/" + bakdir;
			return tofiledir;
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {

		}
	}

	public static class CreatHDFSFile extends Thread {
		private FileSystem fs;
		private Path path;
		private List<FileInfo> fileinfos;
		private int start;
		private int end;
		private FSDataOutputStream os;

		public CreatHDFSFile(FileSystem fs, String path, List<FileInfo> fileinfos, int start, int end) {
			try {
				sleep(1);// 睡1毫秒避免重名
				this.fs = fs;
				this.path = new Path(path + "/" + String.valueOf(new Date().getTime()));
				// System.out.println("写入文件:"+this.path.toString());
				this.fileinfos = fileinfos;
				this.os = this.fs.create(this.path, true, 1024);
			} catch (Exception e) {
				e.printStackTrace();
			}
			this.start = start;
			this.end = end;
		}

		@Override
		public void run() {
			try {
				for (int i = this.start; i < this.end; i++) {
					Text tx = new Text(this.fileinfos.get(i).getFileinfo());
					os.write(tx.getBytes(), 0, tx.getLength());
					os.write(Bytes.toBytes("\n"));
				}
				os.flush();
				os.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}
}
