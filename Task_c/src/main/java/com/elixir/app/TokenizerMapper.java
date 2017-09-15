package com.elixir.app;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public  class TokenizerMapper 
extends Mapper<Object, Text, Text, IntWritable>{

private final static IntWritable one = new IntWritable(1);
private Text word = new Text();

	public void map(Object key, Text value, Context context
	             ) throws IOException,	 InterruptedException {
		String a = value.toString();
		String[] arr = a.split(",");
		word.set(String.valueOf(arr[2]));
		context.write(word, one);
	}
}
