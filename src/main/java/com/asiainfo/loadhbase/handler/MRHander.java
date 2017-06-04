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

import org.apache.commons.net.ftp.FTPFile;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asiainfo.loadhbase.MainApp;
import com.asiainfo.loadhbase.resource.Record;
import com.asiainfo.loadhbase.tool.FtpTools;
import com.asiainfo.loadhbase.tool.LCompress;

public class MRHander extends BaseHandler {
	protected boolean isShardByFileNum;
	protected int maxMapInJob;
	protected int maxTaskPerNode;
	protected int filesPerTask;
	protected String inputHdfsPath;
	protected String maxFileSize;
	protected String maxFileHandlePath;
	protected String inputBakPath;
	protected String detailOutputPath;

	@Override
	public void run() throws Exception {
		initProperty();

		// 获取输入文件信息列表
		List<FileInfo> fileInfoList = new ArrayList<FileInfo>();
		Long totalSize = GetEveryFileInfo(fileInfoList);
		int fileNum = fileInfoList.size();
		if (fileNum == 0) {
			logger.info("task finish,quit!");
			return;
		}
		logger.info("Get fileInfoList success, fileNum:" + fileNum);

		// 输入文件信息分批存放到hdfs的中间文件列表，每个文件对应一个map任务
		Path inputpath = new Path(inputHdfsPath);
		FileInfoToHDFS(inputpath, fileInfoList, totalSize / maxMapInJob, isShardByFileNum);
		logger.info("Put fileInfoList to hdfs success");

		// 作业集群参数配置
		JobConf jobConf = new JobConf(hbaseConfiguration);
		IndividuationJobConf(jobConf);
		jobConf.setNumMapTasks(fileNum / filesPerTask);

		Job job = Job.getInstance(jobConf);
		ConfigJob(job);
		FileInputFormat.addInputPath(job, inputpath);
		logger.info("Config Job success");

		// 执行任务
		long beginTime = System.currentTimeMillis();
		String result = (job.waitForCompletion(true)) ? "Excute job success!" : "Excute job fail!";
		fileSystem.delete(inputpath, true);
		long totalTime = (System.currentTimeMillis() - beginTime) / 1000;
		logger.info(result + " total time(s):" + totalTime);
	}

	private void FileInfoToHDFS(Path inputPath, List<FileInfo> fileInfos, Long avgfilesize, boolean isShardByFileNum)
			throws InterruptedException, IOException {

		if (fileSystem.exists(inputPath)) {
			fileSystem.delete(inputPath, true);
		}
		fileSystem.mkdirs(inputPath, new FsPermission("777"));

		// 有两种策略，按文件数量和文件大小划分任务
		int begPos = 0, endPos = 0, size = fileInfos.size();
		ExecutorService pool = Executors.newFixedThreadPool(50);
		if (isShardByFileNum) {
			for (begPos = 0; begPos < size; begPos += filesPerTask) {
				endPos = begPos + filesPerTask;
				if (begPos + filesPerTask > size) {
					endPos = size;
				}
				pool.execute(new CreatHDFSFile(fileSystem, inputHdfsPath, fileInfos, begPos, endPos));
			}

		} else {
			long currFileSize = 0l;
			for (begPos = 0; begPos < size; begPos++) {
				currFileSize += fileInfos.get(begPos).getSize();
				if (currFileSize >= avgfilesize) {
					endPos = begPos + 1;
					pool.execute(new CreatHDFSFile(fileSystem, inputHdfsPath, fileInfos, begPos, endPos));
					begPos = endPos;
					currFileSize = 0l;
				}
			}
			if (endPos != size) {
				pool.execute(new CreatHDFSFile(fileSystem, inputHdfsPath, fileInfos, begPos, size));
			}
		}
		pool.shutdown();
		while (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
		}
	}

	private void IndividuationJobConf(JobConf jobConf) {
		jobConf.set("isUseDefaultPort", isUseDefaultPort.toString());

		jobConf.setLong("mapred.tasktracker.map.tasks.maximum", maxTaskPerNode);
		jobConf.setLong("mapreduce.input.fileinputformat.split.maxsize", 150 * filesPerTask);
		jobConf.set("maxFileSize", maxFileSize);
		jobConf.set("maxFileHandlePath", maxFileHandlePath);
		jobConf.set("inputBakPath", inputBakPath);
		jobConf.set("detailOutputPath", detailOutputPath);
		
		jobConf.setLong("mapred.min.split.size", 1L);
		jobConf.setInt("mapred.task.timeout", 3600000);
		jobConf.setLong("mapreduce.input.fileinputformat.split.minsize", 1L);
		// mapper失败重试次数，默认是4.在准生产上有机子挂掉,网络联通不了会导致部分失败，必须设置此参数，否则命中失败次数太多会导致整个Job失败。
		jobConf.setInt("mapred.max.map.failures.percent", 20);
		
    	try {
			DefaultStringifier.store(hbaseConfiguration, record ,"record");
		} catch (IOException e) {
			logger.warn("", e);
		}
	}

	private void ConfigJob(Job job) throws IOException {
		job.setJobName("PutHbaseJOB_" + record.getName() + new SimpleDateFormat("yyyyMM").format(new Date()));
		job.setMapperClass(Map.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		job.setNumReduceTasks(0);
		job.setJarByClass(MainApp.class);
		TableMapReduceUtil.addDependencyJars(job);
		TableMapReduceUtil.initCredentials(job);
	}

	private static class CreatHDFSFile extends Thread {
		private FileSystem fileSystem;
		private Path path;
		private List<FileInfo> fileinfos;
		private int start;
		private int end;
		private FSDataOutputStream os;

		public CreatHDFSFile(FileSystem fileSystem, String path, List<FileInfo> fileinfos, int start, int end) {
			try {
				sleep(1);// 睡1毫秒避免重名
				this.fileSystem = fileSystem;
				this.path = new Path(path + "/" + String.valueOf(new Date().getTime()));
				this.fileinfos = fileinfos;
				this.os = this.fileSystem.create(this.path, true, 1024);
				this.start = start;
				this.end = end;
			} catch (Exception e) {
				logger.info("", e);
			}
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
			} catch (IOException e) {
				logger.info("", e);
			} finally {
				try {
					os.close();
				} catch (IOException e) {
					logger.info("", e);
				}
			}
		}

	}
	
	private static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		protected static final Logger logger = LoggerFactory.getLogger(Map.class);
		private final String CHARTSET = "GBK";
		private Record record;
		private Boolean isUseDefaultPort;
		private String maxFileHandlePath;
		private Long maxFileSize;
		private String detail_fileName = "";
		private String detailOutputPath;
		private String inputBakPath;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			logger.info("init");
			isUseDefaultPort = context.getConfiguration().get("isUseDefaultPort").equalsIgnoreCase("true");
			maxFileHandlePath = context.getConfiguration().get("maxFileHandlePath");
			maxFileSize = Long.valueOf(context.getConfiguration().get("maxFileSize"));
			detailOutputPath = context.getConfiguration().get("detailOutputPath");
			inputBakPath = context.getConfiguration().get("inputBakPath");
			// Record.class待验证是否为具体实现类
			record = DefaultStringifier.load(hbaseConfiguration, "key", Record.class);
			System.out.println("set conf secusse!");
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Iterator<Entry<String, FtpTools>> it = FtpTools.ftpClientList.entrySet().iterator();
			logger.info("cleanup:" + FtpTools.ftpClientList.size());
			FtpTools ftptools = null;
			while (it.hasNext()) {
				try {
					ftptools = it.next().getValue();
					if (ftptools.getFtpClient().changeWorkingDirectory(detailOutputPath)) {
						FTPFile[] files = ftptools.getFtpClient().listFiles(detail_fileName);
						if (files.length >= 1) {
							logger.info("rename detailfile:"
									+ detail_fileName
									+ ",result:"
									+ ftptools.rename(detail_fileName,
											detail_fileName.substring(0, detail_fileName.length() - 4)));
						}

					}
					logger.info("ftptools:" + ftptools.toString());
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
			logger.info("contents is:" + value.toString());
			String[] ftpinfo = value.toString().split(":");

			int port = 21;
			if (!isUseDefaultPort)
				port = Integer.valueOf(ftpinfo[1]);

			FtpTools ftp = FtpTools.newInstance(ftpinfo[0], port, ftpinfo[2], ftpinfo[3], ftpinfo[4]);
			int linenum = 0;// 文件总行数,无文件头
			int inputlinenum = 0;// 实际入库行数
			try {
				if (ftp.connectServer()) {
					logger.info("connect success!" + " port:" + port);
					ByteArrayInputStream bin = null;
					FSDataInputStream inStream = null;
					BufferedReader br = null;
					// 获取原文件的行数
					if (maxFileSize < Long.valueOf(ftpinfo[6])) { // 大文件特殊处理
						FileSystem fileSystem = FileSystem.get(context.getConfiguration());
						Path datapath = new Path(maxFileHandlePath + "/" + ftpinfo[5]);
						FSDataOutputStream out = fileSystem.create(datapath);
						ftp.download(ftpinfo[5], out);
						out.close();
						inStream = fileSystem.open(datapath);
						br = new BufferedReader(new InputStreamReader(inStream, CHARTSET));
					} else {
						byte[] bos = ftp.download2Buf(ftpinfo[5]);
						logger.info("download2Buf finish!");
						if (bos.length == 0) {
							logger.info(ftpinfo[5] + "file is empty");
							return;
						}
						if (ftpinfo[5].toLowerCase().endsWith(".z")) {// 解压
							logger.info("ftp:deCompress");
							bos = LCompress.deCompress(bos);
							logger.info("ftp:deCompress finish");
						}
						bin = new ByteArrayInputStream(bos);
						br = new BufferedReader(new InputStreamReader(bin, CHARTSET));
					}

					long starttime = System.currentTimeMillis();
					logger.info("start DealFile:" + ftpinfo[5]);
					// 业务处理
					linenum = record.buildRecord(ftpinfo[5], br, connection);
					long endtime = System.currentTimeMillis();
					logger.info("insert Hbase Finish!recodeCount:" + linenum + ",Time Consuming:"
							+ (endtime - starttime) + "ms.");
					logger.info(ftpinfo[5] + ":process" + linenum);
					recursiondelfile(context, ftp, ftpinfo, 3, linenum, inputlinenum);
				} else {
					logger.info("ftp error!");
				}
			} catch (SocketException e) {
				logger.error("map SocketException:" + e.getMessage());
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				logger.error("map UnsupportedEncodingException:" + e.getMessage());
				e.printStackTrace();
			} catch (IOException e) {
				logger.error("map IOException:" + e.getMessage());
				ExceptionController(value, e);
			} catch (Exception e) {
				ExceptionController(value, e);
			}

		}

		public void ExceptionController(Text value, Exception e) throws InterruptedException {
			logger.error("connect ftp:" + value.toString() + ",error!");
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
				String date = new SimpleDateFormat("yyyyMMddHH24mmss").format(new Date());
				detail_fileName = context.getJobID().toString() + "_" + hostname + "_" + processnum + "_" + date
						+ ".tmp";
			}
			// 文件内容:主机地址|文件名|文件大小|文件记录数|
			String content = ftpinfo[0] + "|" + ftpinfo[4] + "/" + ftpinfo[5] + "|" + ftpinfo[6] + "|" + linenum + "|"
					+ inputlinenum + "\n";
			InputStream is = new ByteArrayInputStream(content.getBytes());
			boolean flag = ftp.writeFile(is, detailOutputPath, detail_fileName);
			is.close();
			if (!flag) {
				throw new IOException("write detail file isSuccess:" + flag);
			}
		}

		private void recursiondelfile(Context context, FtpTools ftp, String[] ftpinfo, int times, int linenum,
				int inputlinenum) {
			if (times <= 0) {
				logger.error("recursiondelfile fail filename:" + ftpinfo[5]);
			} else {
				try {
					if (ftp.connectServer()) {
						if (detailOutputPath!=null && !detailOutputPath.equals("")) {
							detailbak(context, ftpinfo, ftp, linenum, inputlinenum);
						}
						if (inputBakPath==null || inputBakPath.contains("")) {
							logger.info("delete file:" + ftpinfo[5] + "," + ftp.delete(ftpinfo[5]));
						} else {
							String tofiledir = getTofilename(ftpinfo[4], inputBakPath);
							ftp.rename(ftpinfo[4] + "/" + ftpinfo[5], tofiledir + "/" + ftpinfo[5]);
						}
					} else {
						logger.warn("ftp login fail!");
					}
				} catch (IOException e) {
					times = times - 1;
					logger.warn("recursiondelfile IOException:" + e.getMessage());
					e.printStackTrace();
					recursiondelfile(context, ftp, ftpinfo, times, linenum, inputlinenum);
				}
			}
		}

		private String getTofilename(String dir, String inputBakPath) {
			int idx = dir.lastIndexOf("/");
			if (dir.length() == idx + 1)
				idx = dir.lastIndexOf("/", idx - 1);

			String tofiledir = dir.substring(0, idx) + "/" + inputBakPath;
			return tofiledir;
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {

		}
	}

}
