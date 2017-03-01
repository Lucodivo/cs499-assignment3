import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageRatingMapClass extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value,
                       Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        StringTokenizer st = new StringTokenizer(line,",");

        // set word to the value of the movie
        word.set(st.nextToken());
        // throw away user ID token
        st.nextToken();
        // save rating as an integer
        int rating = Integer.parseInt(st.nextToken());
        // record single occurrence of review by user specified by userID
        context.write(word,new IntWritable(rating));

/*
        String line = value.toString();
        StringTokenizer st = new StringTokenizer(line," ");

        while(st.hasMoreTokens()){
            word.set(st.nextToken());
            context.write(word,one);
        }
*/
    }
}