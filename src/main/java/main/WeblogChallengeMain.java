package main;

import flatmap.LineSplitter;
import flatmap.PageHitFilter;
import flatmap.SessionFilter;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.operators.translation.CombineToGroupCombineWrapper;
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
    private static final long TIMEOUT = 15 * 60 * 1000;
    private static final long MINUTE = 60 * 1000;
    // timestamp elb client:port backend:port request_processing_time backend_processing_time response_processing_time elb_status_code backend_status_code received_bytes sent_bytes "request" "user_agent" ssl_cipher ssl_protocol
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

        DataSet<Double> averageSessionLength = userSessionTimes.reduceGroup(new AverageSessionReducer());

        DataSet<Tuple3<String, Integer, Integer>> uniqueHits = sortedGroup
                .reduceGroup(new UniqueHitsReducer(TIMEOUT));

        DataSet<Tuple2<String, Double>> sortedSessions = userSessionTimes
                .reduceGroup(new SortSessionReducer())
                .sortPartition(1, Order.DESCENDING).first(10);


        List<Tuple3<String, Integer, Integer>> userSessionHitsList = userSessionHits.collect();
        List<Double> averageSessionLengthList = averageSessionLength.collect();
        List<Tuple3<String, Integer, Integer>> uniqueHitsList = uniqueHits.collect();
        List<Tuple2<String, Double>> sortedSessionsList = sortedSessions.collect();
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

        System.out.println("Unique Hits Per Session (User, Session#, Unique Hits");
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
