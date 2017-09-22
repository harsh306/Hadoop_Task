import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class H extends Configured implements Tool {
  private H() {}                               // singleton
  static int mypage = 100000;
  static int friends = 20000000;
  static int ave = friends/mypage;
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
    public MyReducer() {}
    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, MyReducer.Context context) throws IOException, InterruptedException {
      HashSet<Integer> friends = new HashSet<Integer>();
      for(IntWritable value: values) {
        int v = value.get();
        friends.add(v);
      }
      int num = friends.size(); 
      if(num > ave) {
        context.write(key, NullWritable.get());
      }
    }
  }
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.out.println("H Friends Output");
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);

    job.setJarByClass(H.class);

    job.setMapperClass(FriendMapper.class);
    job.setReducerClass(MyReducer.class);

    FileInputFormat.setInputPaths(job, args[0]);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    job.waitForCompletion(true);
    return 0;
  }
    
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new H(), args);
    System.exit(res);
  }
}

