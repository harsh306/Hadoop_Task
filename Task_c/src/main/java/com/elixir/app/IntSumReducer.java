package com.elixir.app;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;


public  class IntSumReducer 
     extends Reducer<Text,IntWritable,IntWritable,Text> {
  private IntWritable result = new IntWritable();
  Map<Text,IntWritable> map  =  new HashMap<>();
  private TreeMap<Text, IntWritable> tmaps;
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    tmaps = new TreeMap<>();

  }



  public void reduce(Text key, Iterable<IntWritable> values,
                     Context context
                     ) throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable val : values) {
      sum += val.get();
    }
    result.set(sum);
    tmaps.put(key,  result);
    if(tmaps.size()>10)
    {
      tmaps.remove(tmaps.firstKey());
    }
    //context.write(result, key);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);

    //Map<Text, IntWritable> sortedMap = sortByValues(map);

    int counter = 0;
    for (Text key: (tmaps.descendingKeySet())) {
      context.write( tmaps.get(key),key);
    }
  }

}
