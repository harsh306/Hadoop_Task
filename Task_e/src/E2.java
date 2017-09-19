import java.util.Random;
import java.util.HashSet;
import java.io.IOException;
      
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
  
public class E2 extends Configured implements Tool {
  private E2() {}                               // singleton

  static int maxId = 100000;

  public static class E2Mapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    public E2Mapper() {}
    HashSet<Long> accesses= new HashSet<Long>();

    @Override
    /*
     * Keep only unique access
     */
    public void map(LongWritable key, Text value, E2Mapper.Context context) {
	String a = value.toString();
	String[] arr = a.split(",");
	long byWho = Long.parseLong(arr[1]);
	long who = Long.parseLong(arr[2]);
	accesses.add(byWho*maxId+who);
    }

    @Override
    public void cleanup(E2Mapper.Context context) throws IOException, InterruptedException {
	for(Long key: accesses) {
            context.write(new IntWritable((int)(key/maxId)), new IntWritable((int)(key%maxId)));
	}
    }
  }

  public static class E2Reducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    public E2Reducer() {}
    /*
     * Remove duplicates between mappers
     */
    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, E2Reducer.Context context) throws IOException, InterruptedException {
      HashSet<Integer> accesses= new HashSet<Integer>();
      for(IntWritable value: values) {
        accesses.add(value.get());
      }
      context.write(key, new IntWritable(accesses.size()));
    }
  }

  public static class SortMapper extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {
    public SortMapper() {}
    int[] accesses= new int[maxId];

    @Override
    public void map(IntWritable key, IntWritable value, SortMapper.Context context) {
	accesses[key.get()] = value.get();
    }

    @Override
    public void cleanup(SortMapper.Context context) throws IOException, InterruptedException {
	for(int i = 0; i < maxId; i++) {
            context.write(new IntWritable(i), new IntWritable(accesses[i]));
	}
    }
  }
  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.out.println("E2 Pages AccessLog Output");
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);

    job.setJobName("query");
    job.setJarByClass(E2.class);

    FileInputFormat.setInputPaths(job, args[1]);

    job.setMapperClass(E2Mapper.class);
    ChainReducer.setReducer(job, E2Reducer.class, IntWritable.class, IntWritable.class, IntWritable.class, IntWritable.class, conf);
    ChainReducer.addMapper(job, SortMapper.class, IntWritable.class, IntWritable.class, IntWritable.class, IntWritable.class, conf);
    job.setReducerClass(ChainReducer.class);

    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    job.waitForCompletion(true);
    //Job job2 = Job.getInstance(conf);
    //job2.setJobName("query2");
    //job2.setJarByClass(E2.class);

    //FileInputFormat.setInputPaths(job2, args[1]);

    //job2.setMapperClass(E1Mapper.class);
    //job2.setReducerClass(LongSumReducer.class);

    //FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    //job2.setOutputKeyClass(LongWritable.class);
    //job2.setOutputValueClass(LongWritable.class);
    //job2.waitForCompletion(true);
    return 0;
  }
    
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new E2(), args);
    System.exit(res);
  }
    
}


