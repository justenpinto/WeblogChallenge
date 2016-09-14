package flatmap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * Created by Justen on 2016-09-12.
 */
public class PageHitFilter implements FlatMapFunction<Tuple3<Long, String, String>, Tuple2<String, Integer>>
{
    public void flatMap(Tuple3<Long, String, String> t, Collector<Tuple2<String, Integer>> collector) throws Exception
    {
        collector.collect(new Tuple2<String, Integer>(t.f1, 1));
    }
}
