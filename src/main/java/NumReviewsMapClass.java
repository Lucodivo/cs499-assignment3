import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NumReviewsMapClass extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value,
                       Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        StringTokenizer st = new StringTokenizer(line,",");

        // throw away movie id token
        st.nextToken();
        // add accumulate 1 for userID
        word.set(st.nextToken());
        // throw away movie review rating token
        st.nextToken();
        // record single occurrence of review by user specified by userID
        context.write(word,one);
    }
}