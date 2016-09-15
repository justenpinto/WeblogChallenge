package reduce;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Takes a Tuple of TimeStamp, User, Requests and generates a Tuple list of User, SessionNum, NumberOfHitsForSession.
 */
public class UserSessionHitsReducer implements GroupReduceFunction<Tuple3<Long, String, String>, Tuple3<String, Integer, Integer>>
{
    private final long timeout;

    public UserSessionHitsReducer(long timeout)
    {
        this.timeout = timeout;
    }

    public void reduce(Iterable<Tuple3<Long, String, String>> in, Collector<Tuple3<String, Integer, Integer>> out) throws Exception
    {
        Map<String, Long> lastSessionTimeMap = new HashMap<String, Long>();
        Map<String, Integer> sessionNumMap = new HashMap<String, Integer>();
        Set<String> activeUsers = new HashSet<String>();
        int sessionNum;
        for(Tuple3<Long, String, String> t : in)
        {
            long timestamp = t.f0;
            String user = t.f1;

            if (activeUsers.contains(user))
            {
                long lastSessionTime = lastSessionTimeMap.get(user);
                long diff = timestamp - lastSessionTime;
                if (diff > timeout)
                {
                    // new session
                    sessionNum = getUpdatedSession(user, sessionNumMap);

                }
                else
                {
                    sessionNum = sessionNumMap.get(user);
                }
            }
            else
            {
                // new session
                sessionNum = getUpdatedSession(user, sessionNumMap);
                activeUsers.add(user);
            }
            lastSessionTimeMap.put(user, timestamp);
            out.collect(new Tuple3<String, Integer, Integer>(user, sessionNum, 1));
        }
    }

    private int getUpdatedSession(String user, Map<String, Integer> sessionNumMap)
    {
        int sessionNum = 1;
        if (sessionNumMap.containsKey(user))
        {
            sessionNum = sessionNumMap.get(user) + 1;
        }
        sessionNumMap.put(user, sessionNum);
        return sessionNum;
    }
}