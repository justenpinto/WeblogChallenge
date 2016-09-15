package reduce;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * Take Tuple3<TimeStampMs, User, Request> and return Tuple2<String, SessionLength> for each session.
 */
public class SessionLengthReducer implements GroupReduceFunction<Tuple3<Long, String, String>, Tuple2<String, Long>>
{
    private final long timeout;

    public SessionLengthReducer(long timeout)
    {
        this.timeout = timeout;
    }

    public void reduce(Iterable<Tuple3<Long, String, String>> in, Collector<Tuple2<String, Long>> collector) throws Exception
    {
        long lastSessionTime = 0;
        long startSessionTime = 0;
        boolean isActive = false;

        String user = "";
        for(Tuple3<Long, String, String> t : in)
        {
            long timestamp = t.f0;
            user = t.f1;
            if (isActive)
            {
                long diff = timestamp - lastSessionTime;
                if (diff > timeout)
                {
                    // start of a new session
                    long sessionLength = lastSessionTime - startSessionTime;
                    collector.collect(new Tuple2<String, Long>(user, sessionLength > 0 ? sessionLength : 1));
                    startSessionTime = timestamp;
                }
            }
            else
            {
                startSessionTime = timestamp;
                isActive = true;
            }
            lastSessionTime = timestamp;
        }
        long sessionTime = lastSessionTime - startSessionTime;
        collector.collect(new Tuple2<String, Long>(user, sessionTime > 0 ? sessionTime : 1));
    }
}