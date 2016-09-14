package flatmap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.hadoop.shaded.org.jboss.netty.util.internal.ConcurrentHashMap;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Justen on 2016-09-13.
 */
public class SessionFilter implements FlatMapFunction<Tuple3<Long, String, String>, Tuple3<String, Integer, Integer>>
{
    private final Set<String> activeUsers;
    private final Map<String, Integer> userSessionNumber;
    private final Map<String, Long> lastSessionTime;
    private final long timeout;

    public SessionFilter(long timeout)
    {
        this.activeUsers = new HashSet<String>();
        this.userSessionNumber = new ConcurrentHashMap<String, Integer>();
        this.lastSessionTime = new ConcurrentHashMap<String, Long>();
        this.timeout = timeout;
    }

    public void flatMap(Tuple3<Long, String, String> t, Collector<Tuple3<String, Integer, Integer>> collector) throws Exception
    {
        Long timeStampMs = t. f0;
        String user = t.f1;
        int sessionNum;
        if (activeUsers.contains(user))
        {
            long lastTimeStamp = lastSessionTime.get(user);
            if (timeStampMs - lastTimeStamp > timeout)
            {
                // user has become inactive
                // create new session
                sessionNum = userSessionNumber.get(user);
            }
            else
            {
                // update user
                sessionNum = getUpdatedSessionNumber(user);
            }
        }
        else
        {
            sessionNum = getUpdatedSessionNumber(user);
        }
        collector.collect(createNewSession(user, timeStampMs, sessionNum));
    }

    private int getUpdatedSessionNumber(String user)
    {
        int sessionNum = 1;
        if (userSessionNumber.containsKey(user))
        {
            sessionNum = userSessionNumber.get(user) + 1;
        }
        return sessionNum;
    }

    private Tuple3<String, Integer, Integer> createNewSession(String user, long timeStampMs, int sessionNum)
    {
        userSessionNumber.put(user, sessionNum);
        activeUsers.add(user);
        lastSessionTime.put(user, timeStampMs);
        return new Tuple3<String, Integer, Integer>(user, sessionNum, 1);
    }
}
