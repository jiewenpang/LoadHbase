package com.asiainfo.loadhbase.handler;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
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

public class MRHander extends BaseHandler {
	private boolean isShardByFileNum;
	private int maxTaskInJob;
	private int maxTaskPerNode;
	private int filesPerTask;

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
		FileInfoToHDFS(inputpath, fileInfoList, totalSize / maxTaskInJob, isShardByFileNum);
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
		jobConf.set("maxFileSize", maxFileSize);
		jobConf.set("maxFileHandlePath", maxFileHandlePath);
		jobConf.set("inputBakPath", inputBakPath);
		jobConf.set("detailOutputPath", detailOutputPath);

		// 单位ms，默认10分钟，但此处设置为1小时
		jobConf.setInt("mapred.task.timeout", 3600000);
		// 一般配置为cpu核心数，但需要考虑IO和不影响实时任务
		jobConf.setLong("mapred.tasktracker.map.tasks.maximum", maxTaskPerNode);
		// mapper允许tasktracker失败的百分比
		jobConf.setInt("mapred.max.map.failures.percent", 20);
		
		/*
		 * splitSize=max{max{minSplitSize(默认为1B),mapred.min.split.size}, 
		 * 				 min{mapred.max.split.size(默认Long.MAX_VALUE),dfs.blockSize(默认60MB)}}
		 * 
		 * 分片大小和map的关系，伪代码为：
		 * while (fileSize / splitSize > 1.1) {
		 * 		map.run splitSize;
		 * 		fileSize -= splitSize;
		 * }
		 * map.run fileSize;
		 * 
		 * 预计一行平均小于150字节，则splitSize为maxsize，可以保证每个中间文件只由一个map处理
		 */
		jobConf.setLong("mapred.min.split.size", 1L);
		jobConf.setLong("mapreduce.input.fileinputformat.split.minsize", 1L);
		jobConf.setLong("mapreduce.input.fileinputformat.split.maxsize", 150 * filesPerTask);
		
    	try {
			DefaultStringifier.store(hbaseConfiguration, record ,"record");
		} catch (IOException e) {
			logger.warn("", e);
		}
	}

	private void ConfigJob(Job job) throws IOException {
		job.setJobName("PutHbaseJOB_" + record.getName() +"_"+ new SimpleDateFormat("yyyyMM").format(new Date()));
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
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		protected static final Logger logger = LoggerFactory.getLogger(Map.class);
		private Record record;
		private String maxFileHandlePath;
		private Long maxFileSize;
		private String detailOutputFileName = "";
		private String detailOutputPath;
		private String inputBakPath;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			logger.info("init");
			maxFileHandlePath = context.getConfiguration().get("maxFileHandlePath");
			maxFileSize = Long.valueOf(context.getConfiguration().get("maxFileSize"));
			detailOutputPath = context.getConfiguration().get("detailOutputPath");
			inputBakPath = context.getConfiguration().get("inputBakPath");
			// Record.class待验证是否为具体实现类
			record = DefaultStringifier.load(hbaseConfiguration, "key", Record.class);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			
			NormalCleanUp(record, maxFileHandlePath, detailOutputPath, detailOutputFileName);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			logger.info("contents is:" + value.toString());
			String[] fileInfo = value.toString().split(":");
			
			NormalProcessOneFile(context.getConfiguration(), record, fileInfo, maxFileHandlePath, 
					maxFileSize, detailOutputPath, inputBakPath, detailOutputFileName, context.getJobID().toString());
		}

	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {

		}
	}

}
