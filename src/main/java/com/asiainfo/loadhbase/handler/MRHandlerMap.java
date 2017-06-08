package com.asiainfo.loadhbase.handler;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asiainfo.loadhbase.resource.Record;

public class MRHandlerMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	protected static final Logger logger = LoggerFactory.getLogger(MRHandlerMap.class);
	private Record record;
	private String maxFileHandlePath;
	private Long maxFileSize;
	private String detailOutputFileName = "";
	private String detailOutputPath;
	private String inputBakPath;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		logger.info("init");
		Configuration connection = context.getConfiguration();
		inputBakPath = connection.get("inputBakPath");
		maxFileSize = Long.valueOf(connection.get("maxFileSize"));
		detailOutputPath = connection.get("detailOutputPath");
		maxFileHandlePath = connection.get("maxFileHandlePath");
		
		// Record.class待验证是否为具体实现类
		try {
			record = DefaultStringifier.load(connection, "record", Record.class);

		} catch (java.lang.NullPointerException e) {
			logger.error("hbaseConfiguration:" + connection + ", Record.class" + Record.class, e);
			throw new InterruptedException();
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		BaseHandler.NormalCleanUp(record, maxFileHandlePath, detailOutputPath, detailOutputFileName);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		logger.info("contents is:" + value.toString());
		String[] fileInfo = value.toString().split(":");
		
		BaseHandler.NormalProcessOneFile(context.getConfiguration(), record, fileInfo, maxFileHandlePath, 
				maxFileSize, detailOutputPath, inputBakPath, detailOutputFileName, context.getJobID().toString());
	}
}

