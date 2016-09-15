package main;

import flatmap.LineSplitter;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import reduce.*;

import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Justen on 2016-09-12.
 */
public class WeblogChallengeMain
{
    private static final long MINUTE = 60 * 1000;
    private static final long TIMEOUT = 15 * MINUTE;

    public static void main(String[] args) throws Exception
    {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> logLines = env.readTextFile("C:\\Users\\Justen\\intellij-workspaces\\WeblogChallenge\\data\\" +
                "2015_07_22_mktplace_shop_web_log_sample.log");

        // Grab the log lines. Sort them by timestamp
        SortedGrouping<Tuple3<Long, String, String>> sortedGroup = logLines
                .flatMap(new LineSplitter())
                .groupBy(1)
                .sortGroup(0, Order.ASCENDING);

        // Get the <User,Session#,#Hits> tuple from the logs
        DataSet<Tuple3<String, Integer, Integer>> userSessionHits = sortedGroup
                .reduceGroup(new UserSessionHitsReducer(TIMEOUT))
                .groupBy(0, 1)
                .sum(2);

        // Get the User and all their session lengths
        DataSet<Tuple2<String, Long>> userSessionTimes = sortedGroup.reduceGroup(new SessionLengthReducer(TIMEOUT));

        // Get the average session length of all sessions.
        DataSet<Double> averageSessionLength = userSessionTimes.reduceGroup(new AverageSessionReducer());

        // Get the number of unique hits per sesion (User, Session, Hits)
        DataSet<Tuple3<String, Integer, Integer>> uniqueHits = sortedGroup
                .reduceGroup(new UniqueHitsReducer(TIMEOUT));

        // Get the max session length per user. Sort descending by the session length, and grab the first 10.
        DataSet<Tuple2<String, Double>> sortedSessions = userSessionTimes
                .reduceGroup(new SortSessionReducer())
                .sortPartition(1, Order.DESCENDING).first(10);

        // Call all the collect methods first so that when we print to console, all the gibberish is gone.
        List<Tuple3<String, Integer, Integer>> userSessionHitsList = userSessionHits.collect();
        List<Double> averageSessionLengthList = averageSessionLength.collect();
        List<Tuple3<String, Integer, Integer>> uniqueHitsList = uniqueHits.collect();
        List<Tuple2<String, Double>> sortedSessionsList = sortedSessions.collect();

        // Print each of the metrics requested
        System.out.println("User, Session #, Hits");
        Iterator it = userSessionHitsList.iterator();
        while (it.hasNext())
        {
            System.out.println(it.next());
        }

        System.out.println("Average Session Length (minutes):");
        it = averageSessionLengthList.iterator();
        DecimalFormat df2 = new DecimalFormat("###.##");
        while (it.hasNext())
        {
            System.out.println(df2.format((Double)it.next() / MINUTE));
        }

        System.out.println("Unique Hits Per Session (User, Session#, Unique Hits)");
        it = uniqueHitsList.iterator();
        while (it.hasNext())
        {
            System.out.println(it.next());
        }

        System.out.println("Top 10 Users (User, Minutes): ");
        it = sortedSessionsList.iterator();
        while (it.hasNext())
        {
            Tuple2<String, Double> t = (Tuple2<String, Double>) it.next();
            System.out.println("(" + t.f0 + "," + df2.format(t.f1) + ")");
        }
    }
}
