import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Rating
{
  public static int num;

  public static void main(String[] args)
    throws Exception
  {
    num = Integer.parseInt(args[2]);
    Configuration conf = new Configuration();
    conf.setInt("number", num);
    Job job = new Job(conf, "rating");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setJarByClass(Rating.class);
    job.setMapperClass(Rating.Map.class);
    job.setCombinerClass(Rating.Reduce.class);
    job.setReducerClass(Rating.Reduce.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
  {
    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line, "::");

      this.word.set(tokenizer.nextToken());
      context.write(this.word, one);
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
  {
    int n = 0;

    public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      Configuration conf = context.getConfiguration();
      if (sum > Integer.parseInt(conf.get("number")))
        context.write(key, new IntWritable(sum));
    }
  }
}