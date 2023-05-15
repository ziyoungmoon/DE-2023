import java.io.IOException;
import java.util.*;
import java.time.DayOfWeek;
import java.time.LocalDate;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20190960 
{
	public static class UBERStudent20190960Mapper extends Mapper<Object, Text, Text, Text>
	{
		private Text word = new Text();
		private Text sum = new Text();
		
		private String dayString = "";
		
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			String region = itr.nextToken();
			String date = itr.nextToken();
			StringTokenizer itr2 = new StringTokenizer(date, "/");
			while (itr2.hasMoreTokens()) 
			{
				int month = Integer.parseInt(itr2.nextToken());
				int day = Integer.parseInt(itr2.nextToken());
				int year = Integer.parseInt(itr2.nextToken());
				
				LocalDate date2 = LocalDate.of(year, month, day);
				DayOfWeek dayOfWeek = date2.getDayOfWeek();
				int dayOfWeekNumber = dayOfWeek.getValue();
				
				
				switch(dayOfWeekNumber) {
					case 1: dayString = "MON";
						break;
					case 2: dayString = "TUE";
						break;
					case 3: dayString = "WEN";
						break;
					case 4: dayString = "THR";
						break;
					case 5: dayString = "FRI";
						break;
					case 6: dayString = "SAT";
						break;
					case 7: dayString = "SUN";
						break;
					default: dayString = "Invalid day";
						break;
				}	
			}
			String vehicles = itr.nextToken();
			String trips = itr.nextToken();
			
			sum.set(trips + "," + vehicles);
			word.set(region + "," + dayString);
			context.write(word, sum);
		}
	}
	

	public static class UBERStudent20190960Reducer extends Reducer<Text,Text,Text,Text> 
	{
		private Text result = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			int tripsSum = 0;
			int vehiclesSum = 0;
			
			for (Text val : values) 
			{
				String data[] = (val.toString().split(","));
				tripsSum += Integer.parseInt(data[0]);
				vehiclesSum += Integer.parseInt(data[1]);
				
			}
			result.set(String.valueOf(tripsSum) + "," + String.valueOf(vehiclesSum) );
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "UBERStudent20190960");
		
		job.setJarByClass(UBERStudent20190960.class);
		job.setMapperClass(UBERStudent20190960Mapper.class);
		job.setCombinerClass(UBERStudent20190960Reducer.class);
		job.setReducerClass(UBERStudent20190960Reducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
