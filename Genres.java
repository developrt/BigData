import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Genres
{
  public static int num;

  public static void main(String[] args)
    throws Exception
  {
    String movies = new String(args[2]);

    Configuration conf = new Configuration();
    conf.set("movies", movies);
    Job job = new Job(conf, "Genres");

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setJarByClass(Genres.class);
    job.setMapperClass(Genres.Map.class);
    job.setCombinerClass(Genres.Reduce.class);
    job.setReducerClass(Genres.Reduce.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }

  public static class Map extends Mapper<LongWritable, Text, NullWritable, Text>
  {
    private Text movie = new Text();
    private Text genre = new Text();

    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] tokenizer = line.split("::");
      Configuration conf = context.getConfiguration();
      String movies = conf.get("movies");
      this.movie.set(tokenizer[1]);
      int count = 0;
      if (movies.contains(this.movie.toString())) {
        String[] genres = tokenizer[2].split("\\|");
        int gcnt = 0;
        while (gcnt < genres.length) {
          this.genre.set(genres[gcnt]);
          context.write(NullWritable.get(), this.genre);
          gcnt++;
        }
      }
    }
  }

  public static class Reduce extends Reducer<NullWritable, Text, NullWritable, Text> {
    private Set<String> gset = new TreeSet();

    public void reduce(NullWritable key, Iterable<Text> values, Reducer<NullWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
      for (Text value : values) {
        this.gset.add(value.toString());
      }
      Text fgenre = new Text();
      Iterator genre = this.gset.iterator();
      String tempgenres = new String();
      while (genre.hasNext()) {
        tempgenres = tempgenres + (String)genre.next() + ",";
      }
      int count = 0;
      String genres = new String();
      while (count < tempgenres.length() - 1) {
        genres = genres + tempgenres.charAt(count);
        count++;
      }
      fgenre.set(genres);
      context.write(NullWritable.get(), fgenre);
    }
  }
}