import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageRatingReduceClass extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        double sum = 0;
        double totalReviews = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
            ++totalReviews;
        }

        double avgRating = sum / totalReviews;
        context.write(key, new DoubleWritable(avgRating));
    }
}