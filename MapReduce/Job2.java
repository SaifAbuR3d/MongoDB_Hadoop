import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class Job2 {
	 public static class Job1Mapper extends Mapper<LongWritable, Text, Text, Text> {
	        @Override
	        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	
        		String[] fields = value.toString().split("\t");
        		Text output = new Text("C," + fields[1].toString());
        		context.write(new Text(fields[0]), output);
        	
	        }
	    }

    public static class MoviesMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            String movieID = fields[0].trim();
            String title = fields[1].trim();
            String genre = fields[2].trim();

            if(genre.equalsIgnoreCase("comedy") || genre.equalsIgnoreCase("children")){
            	context.write(new Text(movieID), new Text("M," + title));
            }
        }
    }

    public static class AverageReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
           
            String title = null;
            
            for (Text value : values) {
            	String[] fields = value.toString().split(",");
            	if(fields[0].equals("M")){
            		title=fields[1];
            	}
            	else if(fields[0].equals("C")){
	                sum += Integer.parseInt(fields[1]);
	                count++;
            	}
            }

            if(title!=null && count>0){
	            double averageRating = (double) sum / count;
	            context.write(key, new Text(title + ", " + averageRating));
            }
        }
    }

    public static void main(String[] args) throws Exception {
    	 Configuration conf = new Configuration();
         Job job = Job.getInstance(conf, "Filter Movies and Ratings");

         job.setJarByClass(Job2.class);

         MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Job1Mapper.class);
         MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MoviesMapper.class);

         job.setReducerClass(AverageReducer.class);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(Text.class);
         FileOutputFormat.setOutputPath(job, new Path(args[2]));

         System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}