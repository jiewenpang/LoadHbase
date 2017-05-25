package com.asiainfo.base;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DealReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
		
	}
}
