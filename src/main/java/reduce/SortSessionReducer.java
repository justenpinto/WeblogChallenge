package reduce;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Justen on 2016-09-13.
 */
public class SortSessionReducer implements GroupReduceFunction<Tuple2<String, Long>, Tuple2<String, Double>>
{
    private static final double MINUTES = 60 * 1000d;
    public void reduce(Iterable<Tuple2<String, Long>> iterable, Collector<Tuple2<String, Double>> collector) throws Exception
    {
        Map<String, Long> userMaxMap = new HashMap<String, Long>();
        long LONGEST = Long.MIN_VALUE;
        for (Tuple2<String, Long> t : iterable)
        {
            String user = t.f0;
            if (userMaxMap.containsKey(user))
            {
                if (t.f1 > userMaxMap.get(user))
                {
                    userMaxMap.put(user, t.f1);
                }
            }
            else
            {
                userMaxMap.put(user, t.f1);
            }
        }
        for (Map.Entry<String, Long> kvp : userMaxMap.entrySet())
        {
            collector.collect(new Tuple2<String, Double>(kvp.getKey(), kvp.getValue() / MINUTES));
        }
    }
}
