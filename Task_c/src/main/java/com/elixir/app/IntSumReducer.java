package com.elixir.app;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;


public  class IntSumReducer 
     extends Reducer<Text,IntWritable,Text,IntWritable> {
  private IntWritable result = new IntWritable();
  Map<Text,IntWritable> map  =  new HashMap<Text, IntWritable>();
  public void reduce(Text key, Iterable<IntWritable> values, 
                     Context context
                     ) throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable val : values) {
      sum += val.get();
    }
    result.set(sum);


    map.put(key,  result);
    //context.write(key, result);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);

    Map sortedMap = sortByValues(map);

    int counter = 0;
    for (Object key: (sortedMap.keySet())) {
      counter++;
      if (counter == 20) {
        break;
      }
      context.write((Text) key, (IntWritable) sortedMap.get(key));
    }
  }

public static <K, V extends Comparable<? super V>> Map<K, V>
sortByValues(Map<K, V> map) {
  List<Map.Entry<K, V>> list = new LinkedList<>(map.entrySet());
  Collections.sort( list, new Comparator<Map.Entry<K, V>>() {
    @Override
    public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
      return (o1.getValue()).compareTo(o2.getValue());
    }
  });

  Map<K, V> result = new LinkedHashMap<>();
  for (Map.Entry<K, V> entry : list) {
    result.put(entry.getKey(), entry.getValue());
  }
  return result;
}

}
