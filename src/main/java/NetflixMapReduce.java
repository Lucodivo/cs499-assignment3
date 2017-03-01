import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// docker pull test comment

public class NetflixMapReduce extends Configured implements Tool{

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new NetflixMapReduce(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
            return -1;
        }

        /*
        NUM REVIEWS MAPREDUCE
         */

        Job numReviewsJob = new Job();
        numReviewsJob.setJarByClass(NetflixMapReduce.class);
        numReviewsJob.setJobName("Num Reviews By User Counter");

        FileInputFormat.addInputPath(numReviewsJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(numReviewsJob, new Path(args[1] + "/NumReviews"));

        numReviewsJob.setOutputKeyClass(Text.class);
        numReviewsJob.setOutputValueClass(IntWritable.class);
        numReviewsJob.setOutputFormatClass(TextOutputFormat.class);

        numReviewsJob.setMapperClass(NumReviewsMapClass.class);
        numReviewsJob.setReducerClass(NumReviewsReduceClass.class);

        int returnValue = numReviewsJob.waitForCompletion(true) ? 0:1;

        if(numReviewsJob.isSuccessful()) {
            System.out.println("Num Reviews By User Counter job was successful");
        } else if(!numReviewsJob.isSuccessful()) {
            System.out.println("Num Reviews By User Counter job was not successful");
        }

        /*
        AVERAGE RATINGS MAPREDUCE
         */

        Job avgRatingJob = new Job();
        avgRatingJob.setJarByClass(NetflixMapReduce.class);
        avgRatingJob.setJobName("Average Rating Counter");

        FileInputFormat.addInputPath(avgRatingJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(avgRatingJob, new Path(args[1] + "/AverageRating"));

        avgRatingJob.setOutputKeyClass(Text.class);
        avgRatingJob.setOutputValueClass(DoubleWritable.class);
        avgRatingJob.setOutputFormatClass(TextOutputFormat.class);

        avgRatingJob.setMapperClass(AverageRatingMapClass.class);
        avgRatingJob.setReducerClass(AverageRatingReduceClass.class);

        returnValue = avgRatingJob.waitForCompletion(true) && (returnValue == 0) ? 0:1;

        if(avgRatingJob.isSuccessful()) {
            System.out.println("Average Rating job was successful");
        } else if(!avgRatingJob.isSuccessful()) {
            System.out.println("Average Rating job was not successful");
        }

        return returnValue;
    }
}