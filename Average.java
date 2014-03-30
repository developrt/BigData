package NewStuff;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.StringTokenizer;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class Average
{
  public static void main(String[] args)
    throws Exception
  {
    Configuration conf1 = new Configuration();
    Job job1 = new Job(conf1, "Average");
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(DoubleWritable.class);
    job1.setJarByClass(Average.class);
    job1.setMapperClass(Average.Map.class);
    job1.setCombinerClass(Average.Reduce.class);
    job1.setReducerClass(Average.Reduce.class);

    job1.setInputFormatClass(TextInputFormat.class);
    job1.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1] + "temp"));

    job1.waitForCompletion(true);

    if (job1.isSuccessful()) {
      Configuration conf2 = new Configuration();
      Job job2 = new Job(conf2, "Avg2");

      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(Text.class);
      job2.setJarByClass(Average.class);
      job2.setMapperClass(Average.Map2.class);
      job2.setReducerClass(Average.Reduce2.class);
      job2.setNumReduceTasks(1);
      job2.setInputFormatClass(TextInputFormat.class);
      job2.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job2, new Path(args[1] + "temp"));
      FileOutputFormat.setOutputPath(job2, new Path(args[1]));
      job2.waitForCompletion(true);
    }
  }

  public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable>
  {
    private static final DoubleWritable doubleage = new DoubleWritable();
    private Text age = new Text();
    private Text zipcode = new Text();

    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line, "::");

      this.age.set(tokenizer.nextToken());
      this.age.set(tokenizer.nextToken());
      this.age.set(tokenizer.nextToken());
      this.zipcode.set(tokenizer.nextToken());
      this.zipcode.set(tokenizer.nextToken());
      doubleage.set(Double.parseDouble(this.age.toString()));
      context.write(this.zipcode, doubleage);
    }
  }

  public static class Map2 extends Mapper<LongWritable, Text, Text, Text>
  {
    private TreeMap<String, String> Top10 = new TreeMap();

    static int lineno = 1;

    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException { Text zipcode = new Text();
      DoubleWritable age = new DoubleWritable();
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line, "\t");
      zipcode.set(tokenizer.nextToken());
      age.set(Double.parseDouble(tokenizer.nextToken()));
      this.Top10.put(age.toString() + "::" + String.valueOf(lineno), zipcode.toString());
      if (this.Top10.size() < 0) {
        this.Top10.remove(this.Top10.lastKey());
      }
      lineno += 1; }

    protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      System.out.println("Top10:" + this.Top10);
      for (String keys : this.Top10.keySet()) {
        String temp = keys;
        StringTokenizer tkr = new StringTokenizer(temp, "::");

        String key1 = tkr.nextToken();
        Text T = new Text();
        Text t1 = new Text((String)this.Top10.get(keys));
        T.set(key1);

        context.write(T, t1);
      }
    }
  }

  public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
  {
    public void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
      throws IOException, InterruptedException
    {
      Double n = new Double(0.0D);
      double sum = 0.0D;
      for (DoubleWritable val : values) {
        sum += val.get();
        n = Double.valueOf(n.doubleValue() + 1.0D);
      }
      double average = sum / n.doubleValue();
      context.write(key, new DoubleWritable(average));
    }
  }

  public static class Reduce2 extends Reducer<Text, Text, Text, Text>
  {
    private TreeMap<Double, List<String>> Top10 = new TreeMap();
    int lineno = 0;
    int zcounter = 0;

    public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException
    {
      for (Text t : values) {
        String line = t.toString();
        StringTokenizer tokenizer = new StringTokenizer(line, "$$$$$$");
        String zipcode = tokenizer.nextToken();
        if (this.Top10.containsKey(Double.valueOf(Double.parseDouble(key.toString())))) {
          List temp = (List)this.Top10.get(Double.valueOf(Double.parseDouble(key.toString())));
          temp.add(zipcode);
          this.Top10.put(Double.valueOf(Double.parseDouble(key.toString())), temp);
          this.zcounter += 1;
        }
        else {
          List temp = new ArrayList();
          temp.add(zipcode);
          this.Top10.put(Double.valueOf(Double.parseDouble(key.toString())), temp);
          this.zcounter += 1;
        }
        if (this.zcounter > 105) {
          List temp = (List)this.Top10.get(this.Top10.lastKey());

          temp.remove(temp.size() - 1);
          if (temp.isEmpty()) {
            this.Top10.remove(this.Top10.lastKey());
          }
          else
            this.Top10.put((Double)this.Top10.lastKey(), temp);
        }
      }
    }

    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      System.out.println("Cleaningup");
      Double keys;
      int counter;
      for (Iterator localIterator = this.Top10.descendingKeySet().iterator(); localIterator.hasNext(); 
        counter < ((List)this.Top10.get(keys)).size())
      {
        keys = (Double)localIterator.next();

        String age = Double.toString(keys.doubleValue());
        counter = 0;
        continue;
        context.write(new Text((String)((List)this.Top10.get(keys)).get(counter)), new Text(age));
        counter++;
      }
    }
  }
}