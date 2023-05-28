import java.io.IOException;
import java.util.ArrayList;
import java.io.IOException;
import java.util.StringTokenizer;
import mutipleInput.Join;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UsersRatesMovies {
 ﻿

    public static void main(String[] args) throws Exception {
        
        /* JOB 1 */
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "UsersJoinRatings");
        job1.setJarByClass(UsersRatesMovies.class);
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, UsersMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, RatingsMapper.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[4])); // output of job1
        job1.setReducerClass(UsersJoinRatings.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.waitForCompletion(true);

        /* JOB 2 */
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Final Job");
        job2.setJarByClass(UsersRatesMovies.class);
        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, MoviesMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[4]), TextInputFormat.class, JoinedUsersRatingsMapper.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        job2.setReducerClass(MoviesReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

public static class UsersMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    // (line offset, content of line) --> (UserID,"#U#") 
    // any key-value pair produced is clearly userID having age > 25
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] tuple = value.toString().split(", ");

        String UserID = tuple[0].trim();
        String age = tuple[2].trim();

        if (Integer.parseInt(age) > 25) {
            context.write(new Text(UserID), new Text("#U#"));
        }
    }
}

//-------------------------------------------------------------------------------------------
public static class RatingsMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    // (line offset, content of line) --> (UserID,"MovieID,rating")
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] tuple = value.toString().split(",");

        String UserID = tuple[0].trim();
        String MovieID = tuple[1].trim();
        String rating = tuple[2].trim();

        if (Integer.parseInt(rating) > 2) {
            Text out = new Text(MovieID + "," + rating);
            context.write(new Text(UserID), out);
        }
    }
}
//-------------------------------------------------------------------------------------------

public static class UsersJoinRatings
        extends Reducer<Text, Text, Text, Text> {

    // (UserID, <list of values>) --> (MovieID,rating) 
    // every UserID must exist in Users dataset, no redundancy.
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        boolean oldMan = false;
        ArrayList<Text> info = new ArrayList<>();
        for (Text val : values) {
            if (val.toString().equals("#U#")) {
                oldMan = true;
            } else {
                info.add(val);
            }
        }
        if (!oldMan) {
            return;
        }
        if (info.size() == 0) { // actually no need
            return; 
        }
        //the user is > 25 age, all his ratings are accepted.
        for (Text val : info) {
            String[] str = val.toString().split(",");
            context.write(new Text(str[0]), new Text(str[1]));
        }
    }

}

public static class MoviesMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    // (line offset, line content) --> (movieID,title) 
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] tuple = value.toString().split(",");

        String MovieID = tuple[0];
        String title = tuple[1];
        String genres = tuple[2];

        if (genres.equals("children") || genres.equals("comedy")) {
            Text out = new Text(title);
            context.write(new Text(MovieID), out);
        }
    }
}

public static class JoinedUsersRatingsMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    // (line offset, line content) --> (movieID,"#J#,rating") 
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] tuple = value.toString().split("\t");

        String MovieID = tuple[0];
        String rating = tuple[1];
        Text output = new Text("#J#," + rating);
        context.write(new Text(MovieID), output);
    }
}

public static class MoviesReducer
        extends Reducer<Text, Text, Text, Text> {

    // (MovieID, <list of values>) --> (MovieID, "title avg_rating") 
    // every MovieID must exist in Movies dataset, no redundancy
	
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        float sum = 0;
        int cnt = 0;
        boolean haveRating = false;
        String title = new String("");
        Text out = new Text(new Text(title + '\t' + sum / cnt));
        for (Text val : values) {
            String[] pair = val.toString().split(",");
            if (pair.length == 2) {
                sum += Float.parseFloat(pair[1]);
                cnt++;
                haveRating = true;
            } else {
                title = pair[0];
            }
        }
        if (haveRating && title.length > 0) {
            out = new Text(new Text(title + '\t' + sum / cnt));
        }
        context.write(key, out);
    }

}

}


