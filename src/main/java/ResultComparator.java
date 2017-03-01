import java.util.Comparator;

/**
 * Created by Connor on 3/1/2017.
 */
public class ResultComparator implements Comparator<Result> {
    public int compare(Result result1, Result result2) {
        return (result2.getScore() - result1.getScore() < 0) ? -1:1;
    }
}
