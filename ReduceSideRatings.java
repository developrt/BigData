import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReduceSideRatings
{
  public static void main(String[] args)
    throws Exception
  {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Job job = new Job(conf, "MovieRatings");

    job.setNumReduceTasks(1);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setJarByClass(ReduceSideRatings.RatingsMap.class);
    job.setMapperClass(ReduceSideRatings.RatingsMap.class);
    job.setReducerClass(ReduceSideRatings.RatingsReduce.class);

    MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, ReduceSideRatings.RatingsMap.class);
    MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, ReduceSideRatings.RatingsMap.class);
    MultipleInputs.addInputPath(job, new Path(otherArgs[2]), TextInputFormat.class, ReduceSideRatings.RatingsMap.class);
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

    job.waitForCompletion(true);
  }

  public static class RatingsMap extends Mapper<LongWritable, Text, Text, Text>
  {
    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      InputSplit split = context.getInputSplit();
      Class splitClass = split.getClass();

      FileSplit fileSplit = null;
      if (splitClass.equals(FileSplit.class)) {
        fileSplit = (FileSplit)split;
      }
      else if (splitClass.getName().equals(
        "org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
        try
        {
          Method getInputSplitMethod = splitClass
            .getDeclaredMethod("getInputSplit", new Class[0]);
          getInputSplitMethod.setAccessible(true);
          fileSplit = (FileSplit)getInputSplitMethod.invoke(split, new Object[0]);
        }
        catch (Exception e) {
          throw new IOException(e);
        }
      }

      String filename = fileSplit.getPath().getName();
      String[] line = value.toString().split("::");

      if ((filename.contains("users.dat")) && 
        (line[1].equals("M")))
      {
        context.write(new Text(line[0] + "U"), new Text("U::" + line[0]));
      }

      if (filename.contains("movies.dat")) {
        String[] genres = line[2].split("\\|");

        for (String genre : genres) {
          if ((genre.contains("Action")) || (genre.contains("Drama")))
          {
            context.write(new Text(line[0] + "M"), new Text("M::" + value.toString()));
          }
        }

      }

      if (filename.contains("ratings.dat"))
      {
        context.write(new Text(line[1] + "R"), new Text("R::" + value.toString()));
      }
    }
  }

  public static class RatingsReduce extends Reducer<Text, Text, Text, Text>
  {
    private TreeMap<String, String> userMap = new TreeMap();
    private TreeMap<String, String> movieMap = new TreeMap();
    private List<String> ratingList = new ArrayList();

    public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      for (Text val : values) {
        if (val.toString().charAt(0) == 'U')
        {
          this.userMap.put(key.toString(), val.toString());
        }

        if (val.toString().charAt(0) == 'M')
        {
          this.movieMap.put(key.toString(), val.toString() + "::0.0::0.0");
        }

        if (val.toString().charAt(0) == 'R')
        {
          this.ratingList.add(val.toString());
        }
      }
    }

    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      TreeMap ratingCount = new TreeMap();
      System.out.println("Cleaningup");
      String[] pars;
      for (String ratings : this.ratingList) {
        pars = ratings.split("::");
        String user = pars[1] + "U";

        String movie = pars[2] + "M";

        if ((this.userMap.containsKey(user)) && (this.movieMap.containsKey(movie))) {
          String movieInfo = (String)this.movieMap.get(movie);

          String[] params = movieInfo.split("::");
          Double avg = Double.valueOf(Double.parseDouble(params[4]));
          Double count = Double.valueOf(Double.parseDouble(params[5]));
          avg = Double.valueOf(avg.doubleValue() + Double.parseDouble(pars[3]));
          count = Double.valueOf(count.doubleValue() + 1.0D);
          String newMovieInfo = params[0] + "::" + params[1] + 
            "::" + params[2] + 
            "::" + params[3] + 
            "::" + Double.toString(avg.doubleValue()) + "::" + Double.toString(count.doubleValue());

          this.movieMap.put(movie, newMovieInfo);
        }
      }

      DecimalFormat df = new DecimalFormat("0.00");
      for (String movies : this.movieMap.keySet()) {
        String movieInfo = (String)this.movieMap.get(movies);

        String[] params = movieInfo.split("::");
        Double avg = Double.valueOf(Double.parseDouble(params[4]));
        Double count = Double.valueOf(Double.parseDouble(params[5]));
        avg = Double.valueOf(avg.doubleValue() / count.doubleValue());
        if ((avg.doubleValue() >= 4.4D) && (avg.doubleValue() <= 4.7D)) {
          System.out.println("Found Movie");
          context.write(new Text(params[1] + "\t" + params[2] + "\t" + params[3]), new Text(df.format(avg)));
        }
      }
    }
  }
}