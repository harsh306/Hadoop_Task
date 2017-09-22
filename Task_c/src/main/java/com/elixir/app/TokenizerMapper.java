package com.elixir.app;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public  class TokenizerMapper 
extends Mapper<Object, Text, IntWritable, IntWritable>{

private final static IntWritable one = new IntWritable(1);
	private final static IntWritable two = new IntWritable(1);

	public void map(Object key, Text value, Context context
	             ) throws IOException,	 InterruptedException {
		String a = value.toString();
		String[] arr = a.split(",");
		two.set(Integer.parseInt(arr[2]));
		context.write(two, one);
	}
}
