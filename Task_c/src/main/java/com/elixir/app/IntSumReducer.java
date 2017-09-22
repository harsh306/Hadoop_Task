package com.elixir.app;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

public  class IntSumReducer 
     extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
  private IntWritable result = new IntWritable();
  private TreeMap<Integer, Integer> tmaps;

  public static final Log log = LogFactory.getLog(IntSumReducer.class);
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
      tmaps = new TreeMap<>();
  }
public void reduce(IntWritable key, Iterable<IntWritable> values,
                     Context context
                     ) throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable val : values) {
      sum += val.get();
    }
    tmaps.put(sum,key.get());
    if(tmaps.size()>10){
        tmaps.remove(tmaps.firstKey());
    }
    //result.set(sum);
    //context.write(result, key);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);
    for (Integer key: (tmaps.keySet())) {
      context.write(new IntWritable(key), new IntWritable(tmaps.get(key)));
    }
  }

}
