import java.util.Random;
import java.io.IOException;
      
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
  
public class E1 extends Configured implements Tool {
  private E1() {}                               // singleton

  static int maxId = 100000;
  public static class E1Mapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
    public E1Mapper() {}
    long[] accesses= new long[maxId];

    /*
     * Use byWho as key to count
     */
    @Override
    public void map(LongWritable key, Text value, E1Mapper.Context context) {
	String a = value.toString();
	String[] arr = a.split(",");
	int byWho = Integer.parseInt(arr[1]);
	accesses[byWho] ++;
    }

    @Override
    public void cleanup(E1Mapper.Context context) throws IOException, InterruptedException {
	for(int i = 0; i < maxId; i++) {
            context.write(new LongWritable(i), new LongWritable(accesses[i]));
	}
    }
  }

  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.out.println("E1 Pages AccessLog Output");
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);

    job.setJarByClass(E1.class);

    FileInputFormat.setInputPaths(job, args[1]);

    job.setMapperClass(E1Mapper.class);
    job.setReducerClass(LongSumReducer.class);

    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(LongWritable.class);
    job.waitForCompletion(true);
    return 0;
  }
    
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new E1(), args);
    System.exit(res);
  }
    
}

