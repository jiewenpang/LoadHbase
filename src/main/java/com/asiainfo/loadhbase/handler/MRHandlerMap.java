package com.asiainfo.loadhbase.handler;

import java.io.IOException;

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
		maxFileHandlePath = context.getConfiguration().get("maxFileHandlePath");
		maxFileSize = Long.valueOf(context.getConfiguration().get("maxFileSize"));
		detailOutputPath = context.getConfiguration().get("detailOutputPath");
		inputBakPath = context.getConfiguration().get("inputBakPath");
		// Record.class����֤�Ƿ�Ϊ����ʵ����
		try {
			record = DefaultStringifier.load(context.getConfiguration(), "record", Record.class);

		} catch (java.lang.NullPointerException e) {
			logger.error("hbaseConfiguration:" + context.getConfiguration() + ", Record.class" + Record.class, e);
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
