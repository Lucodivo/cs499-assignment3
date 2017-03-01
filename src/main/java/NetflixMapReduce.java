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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.PriorityQueue;

// docker pull test comment

public class NetflixMapReduce extends Configured implements Tool{

    private final static String numRevDir = "/NumReviews";
    private final static String avgRatDir = "/AvgRatings";
    private final static String numRevResults = numRevDir + "/part-r-00000";
    private final static String avgRatResults = avgRatDir + "/part-r-00000";

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new NetflixMapReduce(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
            return -1;
        }

        int returnValue = numReviewsMapReduce(args[0], args[1]);
        returnValue = (avgRatingsMapReduce(args[0], args[1]) == 1) && (returnValue == 0) ? 0:1;

        calcTop10Movies(args[1]);
        System.out.println();
        calcTop10Reviewers(args[1]);

        return 0;
    }

    private int avgRatingsMapReduce(String inputPath, String outputPath) throws Exception {
        /// AVERAGE RATINGS MAPREDUCE

        Job avgRatingJob = new Job();
        avgRatingJob.setJarByClass(NetflixMapReduce.class);
        avgRatingJob.setJobName("Average Rating Counter");

        FileInputFormat.addInputPath(avgRatingJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(avgRatingJob, new Path(outputPath + avgRatDir));

        avgRatingJob.setOutputKeyClass(Text.class);
        avgRatingJob.setOutputValueClass(DoubleWritable.class);
        avgRatingJob.setOutputFormatClass(TextOutputFormat.class);

        avgRatingJob.setMapperClass(AverageRatingMapClass.class);
        avgRatingJob.setReducerClass(AverageRatingReduceClass.class);

        int returnValue = avgRatingJob.waitForCompletion(true) ? 0:1;

        if(avgRatingJob.isSuccessful()) {
            System.out.println("Average Rating job was successful");
        } else if(!avgRatingJob.isSuccessful()) {
            System.out.println("Average Rating job was not successful");
        }

        return returnValue;
    }

    private int numReviewsMapReduce(String inputPath, String outputPath) throws Exception {

        /// NUM REVIEWS MAPREDUCE
        Job numReviewsJob = new Job();
        numReviewsJob.setJarByClass(NetflixMapReduce.class);
        numReviewsJob.setJobName("Num Reviews By User Counter");

        FileInputFormat.addInputPath(numReviewsJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(numReviewsJob, new Path(outputPath + numRevDir));

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

        return returnValue;
    }

    private void calcTop10Movies(String outputPath) {

        /// Calculate results of top 10 highest average movies

        PriorityQueue<Result> avgRatingQueue = new PriorityQueue<Result>(100, new ResultComparator());
        BufferedReader reader = null;

        try {
            File file = new File(new Path(outputPath + avgRatResults).toString());
            reader = new BufferedReader(new FileReader(file));

            String line;
            while((line = reader.readLine()) != null) {
                String [] tokens = line.split("\t");
                avgRatingQueue.add(new Result(tokens[0], Double.parseDouble(tokens[1])));
            }
        } catch (IOException e){
            e.printStackTrace();
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println("--- Top 10 Highest Average Rated Movies ---");
        for(int i = 1; i < 11; ++i) {
            Result r = avgRatingQueue.remove();
            System.out.println("#" + i + " " + r.getId() + " - " + r.getScore());
        }
    }

    private void calcTop10Reviewers(String outputPath) {
        /// Calculate results of top 10 reviewers

        PriorityQueue<Result> numReviewsQueue = new PriorityQueue<Result>(100, new ResultComparator());
        BufferedReader reader = null;

        try {
            File file = new File(new Path(outputPath + numRevResults).toString());
            reader = new BufferedReader(new FileReader(file));

            String line;
            while((line = reader.readLine()) != null) {
                String [] tokens = line.split("\t");
                numReviewsQueue.add(new Result(tokens[0], Double.parseDouble(tokens[1])));
            }
        } catch (IOException e){
            e.printStackTrace();
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println("--- Top 10 Reviewers ---");
        for(int i = 1; i < 11; ++i) {
            Result r = numReviewsQueue.remove();
            System.out.println("#" + i + " " + r.getId() + " - " + r.getScore());
        }
    }
}