package reduce;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Takes Tuple3<TimeStamp, String, Request> and returns Tuple3<User, SessionNum, #UniqueHits>.
 */
public class UniqueHitsReducer implements GroupReduceFunction<Tuple3<Long, String, String>, Tuple3<String, Integer, Integer>>
{
    private final long timeout;

    public UniqueHitsReducer(long timeout)
    {
        this.timeout = timeout;
    }

    public void reduce(Iterable<Tuple3<Long, String, String>> in, Collector<Tuple3<String, Integer, Integer>> out) throws Exception
    {
        long lastSessionTime = 0;
        Set<String> userUniqueRequests = new HashSet<String>();
        boolean isActive = false;
        int sessionNum = 1;
        String user = "";
        for(Tuple3<Long, String, String> t : in)
        {
            long timestamp = t.f0;
            user = t.f1;
            String request = t.f2;

            if (isActive)
            {;
                long diff = timestamp - lastSessionTime;
                if (diff > timeout)
                {
                    // Previous session has timed out
                    // Gather information and collect
                    out.collect(new Tuple3<String, Integer, Integer>(user, sessionNum, userUniqueRequests.size()));

                    // Reset session informatoin
                    userUniqueRequests.clear();
                    sessionNum++;
                    userUniqueRequests.add(request);
                }
                else
                {
                    if (!userUniqueRequests.contains(request))
                    {
                        userUniqueRequests.add(request);
                    }
                }
            }
            else
            {
                // new session
                sessionNum++;
                userUniqueRequests.add(request);
                isActive = true;
            }
            lastSessionTime = timestamp;
        }
        out.collect(new Tuple3<String, Integer, Integer>(user, sessionNum, userUniqueRequests.size()));
    }
}