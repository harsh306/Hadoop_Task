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

public class G extends Configured implements Tool {
  private G() {}                               // singleton
  static int thresholdTime;
  public static class AccessMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    public AccessMapper() {}
    @Override
    public void map(LongWritable key, Text value, AccessMapper.Context context) throws IOException, InterruptedException {
        String a = value.toString();
        String[] arr = a.split(",");
        int who = Integer.parseInt(arr[1]);
        int time = Integer.parseInt(arr[4]);
        context.write(new IntWritable(who), new IntWritable(time));
    }
  }
  public static class MyReducer extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
    public MyReducer() {}
    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, MyReducer.Context context) throws IOException, InterruptedException {
      int latest = thresholdTime;
      for(IntWritable value: values) {
        int v = value.get();
        if(v > latest) {
	  latest = v;
        }
      }
      if(latest <= thresholdTime) {
        context.write(key, NullWritable.get());
      }
    }
  }
  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.out.println("G thresholdTime AccessLog Output");
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);

    job.setJarByClass(G.class);

    job.setMapperClass(AccessMapper.class);
    job.setReducerClass(MyReducer.class);

    thresholdTime = Integer.parseInt(args[0]);
    FileInputFormat.setInputPaths(job, args[1]);
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    job.waitForCompletion(true);
    return 0;
  }
    
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new G(), args);
    System.exit(res);
  }
}

