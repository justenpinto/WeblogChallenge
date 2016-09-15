package reduce;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Takes a list of Tuple2<String, SessionLength> and returns the average session length for the given user
 */
public class AverageSessionReducer implements GroupReduceFunction<Tuple2<String, Long>, Double>
{
    public void reduce(Iterable<Tuple2<String, Long>> in, Collector<Double> collector) throws Exception
    {
        long sum = 0;
        long N = 0;

        for (Tuple2<String, Long> t : in)
        {
            sum += t.f1;
            N++;
        }
        collector.collect((double)sum / N);
    }
}