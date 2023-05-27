import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;
public class IMDBStudent20190960
{
   public static class Movie {
      String title;
      double avg;
      public Movie(String title, double avg) {
         this.title = title;
         this.avg = avg;
      }
   }
   public static void insertMovie(PriorityQueue q, String title, double avg, int topK) {
      Movie movie_head = (Movie) q.peek();
      if ( q.size() < topK || movie_head.avg < avg )
      {
         Movie m = new Movie(title, avg);
         q.add( m );
         if( q.size() > topK ) 
            q.remove();
      }
   }
   public static class MovieComparator implements Comparator<Movie> {
      public int compare(Movie x, Movie y) {
         if ( x.avg > y.avg ) return 1;
         if ( x.avg < y.avg ) return -1;
         return 0;
      }
   }
   
   public static class IMDBMapper extends Mapper<Object, Text, Text, Text>
   {
      boolean fileM = true;
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException
      {
         
         String[] data = value.toString().split("::");
         Text outputKey = new Text();
         Text outputValue = new Text();
         String joinKey = "";
         String o_value = "";
         String title = "";
         String rating = "";
         String genre = "";
         
         if( fileM ) {
            joinKey = data[0];
            title = data[1];
            genre = data[2];
            StringTokenizer itr2 = new StringTokenizer(genre, "|");
            
            while (itr2.hasMoreTokens()) 
            {
               if (itr2.nextToken().equals("Fantasy")) {
                  o_value = title;
                  outputKey.set(joinKey);
                  outputValue.set(o_value);
                  context.write( outputKey, outputValue );
               }
            }
         }
         else {
            joinKey = data[1];
            rating = data[2];
            o_value = rating;
            outputKey.set(joinKey);
            outputValue.set(o_value);
            context.write( outputKey, outputValue );
         }
         
      }   
      protected void setup(Context context) throws IOException, InterruptedException
      {
         String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
         if ( filename.indexOf( "movies.dat" ) != -1 ) 
            fileM = true;
         else 
            fileM = false;
      }
   }
   
   public static class IMDBReducer extends Reducer<Text,Text,Text,DoubleWritable>
   {
      private PriorityQueue<Movie> queue ;
      private Comparator<Movie> comp = new MovieComparator();
      private int topK;
      
      Text reduce_key = new Text();
      DoubleWritable reduce_result = new DoubleWritable();
      
      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
         
         int sum = 0;
         int count = 0;
         double avg = 0;
         int flag = 0;
         String title = "";

         for (Text val : values) {
            try {
               sum += Integer.parseInt(val.toString());
               count++;
            } catch (NumberFormatException e) {
               flag = 1;
               title = val.toString();
            }
         } 
         if (flag == 1) {
            avg = (double)sum / count;
            insertMovie(queue, title, avg, topK);
         }
      }
      protected void setup(Context context) throws IOException, InterruptedException {
         Configuration conf = context.getConfiguration();
         topK = conf.getInt("topK", -1);
         queue = new PriorityQueue<Movie>( topK , comp);
      }
      protected void cleanup(Context context) throws IOException, InterruptedException {
         while( queue.size() != 0 ) {
            Movie m = (Movie) queue.remove();
            reduce_key.set(m.title);
            reduce_result.set(m.avg);
            context.write(reduce_key, reduce_result);
         }
      }
   }
   public static void main(String[] args) throws Exception
   {
      Configuration conf = new Configuration();
      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      
      if (otherArgs.length != 3)
      {
         System.err.println("Usage: ReduceSideJoin2 <in> <out> <topK>");
         System.exit(2);
      }
      
      conf.setInt("topK", Integer.parseInt(otherArgs[2]));
      Job job = new Job(conf, "IMDB");
   
      job.setJarByClass(IMDBStudent20190960.class);
      job.setMapperClass(IMDBMapper.class);
      job.setReducerClass(IMDBReducer.class);
      
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
   
      
      FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
      FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
      FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}
