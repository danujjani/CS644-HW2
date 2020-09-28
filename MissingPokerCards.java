import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MissingPokerCards{
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
     	  String s = value.toString();
          String[] split = s.split(" ");
	  context.write(new Text(split[0]), new IntWritable(Integer.parseInt(split[1])));
        }
    }
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> value, Context context)throws IOException, InterruptedException{
   	  int i = 1,existing_card = 0;
          ArrayList<Integer> deck = new ArrayList<Integer>();

          for(i=1;i<=13;++i){
            deck.add(i);
          }

          for(IntWritable cards : value){
            existing_card = cards.get();
            if(deck.contains(existing_card)){
              deck.remove(deck.indexOf(existing_card));
            }
          }

          for(i=0;i<deck.size();++i){
            context.write(key, new IntWritable(deck.get(i)));
          }
        }
      }
	  public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	    Job job = new Job(conf, "MissingPokerCards");
    	    job.setJarByClass(MissingPokerCards.class);
    	    job.setMapperClass(Map.class);
    	    job.setCombinerClass(Reduce.class);
    	    job.setReducerClass(Reduce.class);
    	    job.setOutputKeyClass(Text.class);
    	    job.setOutputValueClass(IntWritable.class);
    	    for (int i = 0; i < otherArgs.length - 1; ++i) {
      	        FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
            }
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
