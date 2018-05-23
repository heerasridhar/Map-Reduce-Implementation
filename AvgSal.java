import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.io.WritableUtils;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;

import java.io.DataOutput;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.fs.Path;

import org.apache.commons.io.FileUtils;





public class AvgSal {

	

	public static class Map extends Mapper<LongWritable,Text,Text,FloatWritable>{



		public void map(LongWritable key, Text data,

				Context context)

				throws IOException,InterruptedException {

			try{

			String line = data.toString();
String rec[] = line.split(",");
//while (rec[]) {
String yr = rec[0];
String year = yr.trim().substring(0,4);

if(key.get() >0)
{
//System.out.println("the value of year is "+ year);
String state = rec[5];

//System.out.println("the value of name of state is %s" + nameOfState);
String gen = rec[69];

Float salary = Float.parseFloat(rec[72]);
context.write(new Text(state+ ""+gen),new FloatWritable(salary));


}

}
catch(Exception e)
{
e.printStackTrace();
}
}

			
}
public static class Reduce extends Reducer<Text,FloatWritable,Text,FloatWritable>{



		public void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException,InterruptedException {

			//float sum=0.0;
            
			float sum = (float)0.0;
			int count = 0;
			// TODO Auto-generated method stub

			for(FloatWritable x: values)

			{

				sum+=x.get();
				 count ++;

			}
             float avg = (float)sum/count;
			context.write(key, new FloatWritable(avg));

			

		}

		

	}

	

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//JobConf conf = new JobConf(WordCount.class);
			Configuration conf = new Configuration();
			Job j2 = Job.getInstance(conf);
			j2.setJobName("Average Salary");
			j2.setJarByClass(AvgSal.class);
			//Mapper input and output
			j2.setMapOutputKeyClass(Text.class);
			j2.setMapOutputValueClass(FloatWritable.class);
			//Reducer input and output
			j2.setOutputKeyClass(Text.class);
			j2.setOutputValueClass(FloatWritable.class);
			//file input and output of the whole program
			j2.setInputFormatClass(TextInputFormat.class);
			j2.setOutputFormatClass(TextOutputFormat.class);
			//Set the mapper class
			j2.setMapperClass(Map.class);
			//Set the reducer class
			j2.setReducerClass(Reduce.class);
			FileOutputFormat.setOutputPath(j2, new Path(args[1]));
			FileInputFormat.addInputPath(j2, new Path(args[0]));
			 j2.waitForCompletion(true);
			//int code = j2.waitForCompletion(true) ? 0 : 1;
    		//System.exit(code);

	}

}
