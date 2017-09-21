import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import java.io.IOException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class F extends Configured implements Tool {
  private F() {}                               // singleton
  static int maxId = 100000;
  public static class AccessMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    public AccessMapper() {}
    @Override
    public void map(LongWritable key, Text value, AccessMapper.Context context) throws IOException, InterruptedException {
        String a = value.toString();
        String[] arr = a.split(",");
        int who = Integer.parseInt(arr[1]);
        int friend = Integer.parseInt(arr[2]);
        context.write(new IntWritable(who), new IntWritable(friend+maxId));
    }
  }
  public static class FriendMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    public FriendMapper() {}
    @Override
    public void map(LongWritable key, Text value, FriendMapper.Context context) throws IOException, InterruptedException {
        String a = value.toString();
        String[] arr = a.split(",");
        int who = Integer.parseInt(arr[1]);
        int friend = Integer.parseInt(arr[2]);
        context.write(new IntWritable(who), new IntWritable(friend));
    }
  }
  public static class MyReducer extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
    // -1 access, 1 friend but not accessed
    public MyReducer() {}
    /*
     * Remove duplicates between mappers
     */
    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, MyReducer.Context context) throws IOException, InterruptedException {
      int[] accesses = new int[maxId];
      for(IntWritable value: values) {
        int v = value.get();
        if(v >= maxId) {
          // access
          accesses[v-maxId] = -1;
        }
        else if(accesses[v] != -1 ) {
          // friend but not accessed
          accesses[v] = 1;
        }
      }
      boolean flag = false;
      for(int i = 0; i < maxId; i++) {
        if(accesses[i] == 1) {
          flag = true;
        }
      }
      if(flag) {
        context.write(key, NullWritable.get());
      }
    }
  }
  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.out.println("F Friends AccessLog Output");
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);

    job.setJarByClass(F.class);
    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FriendMapper.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AccessMapper.class);

    job.setReducerClass(MyReducer.class);

    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    job.waitForCompletion(true);
    return 0;
  }
    
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new F(), args);
    System.exit(res);
  }
}

