import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class Job1 {

    public static class RatingsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            int rating = Integer.parseInt(fields[2].trim());
            String MovieID = fields[1];
            if (rating <= 2) {
                return;
            }

            context.write(new Text(fields[0].trim()), new Text("R," + MovieID + "," + rating));
        }
    }

    public static class UsersMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            int age = Integer.parseInt(fields[2].trim());
            if (age <= 25) {
                return; // Skip users below or equal to 25
            }

            context.write(new Text(fields[0].trim()), new Text("U"));
        }
    }

    public static class FilterReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean movieFound = false;
            boolean userFound = false;
            List<String[]> list = new ArrayList<>();
            
            for (Text value : values) {
                String[] parts = value.toString().split(",");
                
                if (parts[0].equals("R")) {
                    movieFound = true;
                    list.add(parts);
                } else if (parts[0].equals("U")) {
                    userFound = true;
                }
            }
            if (movieFound && userFound) {
            	for(String[] it: list){
            		String movieID = it[1];
            		String mRating = it[2];
            		context.write(new Text(movieID), new Text(mRating));
            	}
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Filter Movies and Ratings");

        job.setJarByClass(Job1.class);


        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, UsersMapper.class);

        job.setReducerClass(FilterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
