import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
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

public class MapsideTop10
{
  public static void main(String[] args)
    throws Exception
  {
    Configuration conf = new Configuration();
    String Path = "/Spring2014_HW-1/input_HW-1";
    DistributedCache.addCacheFile(new URI(Path + "/users.dat"), conf);
    Job job = new Job(conf, "NewTop10");

    job.setNumReduceTasks(1);

    URI[] lfs = DistributedCache.getCacheFiles(job.getConfiguration());

    System.out.println(lfs[0]);
    System.out.println("File added to cache successfully");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setJarByClass(MapsideTop10.Map.class);
    job.setMapperClass(MapsideTop10.Map.class);

    job.setReducerClass(MapsideTop10.Reduce.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }

  public static class Map extends Mapper<LongWritable, Text, Text, Text>
  {
    private String userid;
    private String movieid;
    private TreeMap<Integer, String> userinfo = new TreeMap();
    private TreeMap<Integer, List<String>> NewTop10 = new TreeMap();

    public void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
      for (Path p : files) {
        Scanner s = new Scanner(new File(p.toString()));
        while (s.hasNext()) {
          String line = s.nextLine();
          if (!line.isEmpty()) {
            String[] itr = line.split("::");
            this.userinfo.put(Integer.valueOf(Integer.parseInt(itr[0])), line);
          }
        }
      }
    }

    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line, "::");

      this.userid = tokenizer.nextToken().toString();
      this.movieid = tokenizer.nextToken().toString();

      if (!this.NewTop10.containsKey(Integer.valueOf(Integer.parseInt(this.userid)))) {
        List movielist = new ArrayList();
        movielist.add(this.movieid);
        this.NewTop10.put(Integer.valueOf(Integer.parseInt(this.userid)), movielist);
      }
      else {
        List movielist = (List)this.NewTop10.get(Integer.valueOf(Integer.parseInt(this.userid)));
        movielist.add(this.movieid);
        this.NewTop10.put(Integer.valueOf(Integer.parseInt(this.userid)), movielist);
      }
    }

    protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      Iterator localIterator2;
      for (Iterator localIterator1 = this.NewTop10.keySet().iterator(); localIterator1.hasNext(); 
        localIterator2.hasNext())
      {
        Integer keys = (Integer)localIterator1.next();
        List movielist = new ArrayList();
        movielist = (List)this.NewTop10.get(keys);
        localIterator2 = movielist.iterator(); continue; String movies = (String)localIterator2.next();
        String line = (String)this.userinfo.get(keys) + "::" + movies;

        context.write(new Text(Integer.toString(keys.intValue())), new Text(line));
      }
    }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text>
  {
    private TreeMap<Integer, List<String>> NewTop10 = new TreeMap();

    public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      int sum = 0;
      String line = "";
      for (Text val : values)
      {
        line = val.toString();
        sum++;
      }

      if (!this.NewTop10.containsKey(Integer.valueOf(sum))) {
        List temp = new ArrayList();
        temp.add(line);
        this.NewTop10.put(Integer.valueOf(sum), temp);
      }
      else {
        List temp = (List)this.NewTop10.get(Integer.valueOf(sum));
        temp.add(line);
      }
    }

    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      int counter = 0;

      String msg = "UserId Age Gender \tCount";
      context.write(new Text(msg), new Text(""));
      for (Integer keys : this.NewTop10.descendingKeySet())
      {
        List ops = (List)this.NewTop10.get(keys);
        for (String line : ops) {
          String[] fop = line.split("::");
          String lines = fop[0] + " " + fop[2] + "\t\t" + fop[1] + " ";
          context.write(new Text(lines), new Text(Integer.toString(keys.intValue())));
          counter++;
        }

        if (counter == 10)
          break;
      }
    }
  }
}