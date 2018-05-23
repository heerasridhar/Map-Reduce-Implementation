import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class Age {
 
  public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text>{
 
    
    
 
    public void map(LongWritable key, Text value, Context context ) throws IOException,InterruptedException {
    try{
      Text word = new Text();
      String newage;
        String line = value.toString();
        String[] data = line.split(",");
        String serialno=data[0];
        String year=serialno.substring(0,4); //gets the year for each record
        Integer age=Integer.parseInt(data[8]); //gets the age for each record
  if (age >= 0 && age <= 9) {
      newage = "0 - 09";
    } else if (age >= 10 && age <= 19) {
      newage = "10 - 19";
    } else if (age >= 20 && age <= 29) {
      newage = "20 - 29";
    } else if (age >= 30 && age <= 39) {
      newage = "30 - 39";
    } else if (age >= 40 && age <= 49) {
      newage = "40 - 49";
    } else if (age >= 50 && age <= 59) {
      newage = "50 - 59";
    } else if (age >= 60 && age <= 69) {
      newage = "60 - 69";
    } else if (age >= 70 && age <= 79) {
      newage = "70 - 79";
    } else if (age >= 80 && age <= 89) {
      newage = "80 - 89";
    } else if (age >= 90 && age <= 99) {
      newage = "90 - 99";
    } else {
      newage = "others";
  }

        if (!newage.equals("others")) {
      word.set(new Text(year) + "," + new Text(newage));
      context.write(word, new Text("1"));
        }
  }catch(Exception e){
  System.err.println(e);
  }
  }
  }

  public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text word, Iterable<Text> values,Context context) throws IOException, InterruptedException 
    {
      int count = 0;
      for(Text val:values){
      count += 1;
    }
    context.write(word, new Text(""+count));
    }
  }
 
  public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException {
  Configuration conf = new Configuration();
  Job j3 = Job.getInstance(conf);
  j3.setJobName("# of peoples");
  j3.setJarByClass(Age.class);
  //Mapper input and output
  j3.setMapOutputKeyClass(Text.class);
  j3.setMapOutputValueClass(Text.class);
  //Reducer input and output
  j3.setOutputKeyClass(Text.class);
  j3.setOutputValueClass(Text.class);
  //file input and output of the whole program
  j3.setInputFormatClass(TextInputFormat.class);
  j3.setOutputFormatClass(TextOutputFormat.class);
  //Set the mapper class
  j3.setMapperClass(TokenizerMapper.class);
  //set the combiner class for custom combiner
  //j3.setCombinerClass(WordReducer.class);
  //Set the reducer class
  j3.setReducerClass(IntSumReducer.class);
  //set the number of reducer if it is zero means there is no reducer
  j3.setNumReduceTasks(2);
  FileOutputFormat.setOutputPath(j3, new Path(args[1]));
  FileInputFormat.addInputPath(j3, new Path(args[0]));
  j3.waitForCompletion(true);
  }
}
