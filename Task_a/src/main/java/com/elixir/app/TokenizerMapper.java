package com.elixir.app;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public  class TokenizerMapper 
extends Mapper<Object, Text, Text, Text>{

private Text word = new Text();
private Text word2 = new Text();

	public void map(Object key, Text value, Context context
	             ) throws IOException,	 InterruptedException {

		String a = value.toString();
		String[] arr = a.split(",");
		if (Integer.parseInt(arr[3]) == 4){
			word.set(arr[1]+","+arr[4]);
			word2.set(String.valueOf(arr[0]));
			context.write(word2,word);
		}

	}
}
