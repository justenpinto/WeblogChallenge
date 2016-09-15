package flatmap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;

/**
 * Takes a String representing a log line and outputs Tuple of TimeStamp, String, Request.
 */
public class LineSplitter implements FlatMapFunction<String, Tuple3<Long, String, String>>
{
    private static final Set<Integer> indices = new HashSet<Integer>();

    public LineSplitter()
    {
        indices.add(0);
        indices.add(2);
        indices.add(11);
    }

    public void flatMap(String line, Collector<Tuple3<Long, String, String>> collector) throws Exception
    {
        boolean startQuote = false;
        StringBuilder sb = new StringBuilder();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'");
        sb.setLength(0);
        Tuple3<Long, String, String> tuple = new Tuple3();
        int index = 0;
        int tupleIndex = 0;
        int i = 0;
        try
        {
            for (i = 0; i < line.length(); i++)
            {
                char c = line.charAt(i);
                if (c == '"' && line.charAt(i-1) == ' ')
                {
                    startQuote = true;
                }
                else if (c == '"' && line.charAt(i+1) == ' ')
                {
                    // end quote
                    if (indices.contains(index++))
                    {
                        Object value;
                        if (tupleIndex == 0)
                        {
                            value = df.parse(sb.toString()).getTime();
                        }
                        else if (tupleIndex == 1)
                        {
                            value = sb.toString();
                            value = ((String)value).substring(0, ((String) value).indexOf(":"));
                        }
                        else
                        {
                            value = sb.toString();
                        }
                        tuple.setField(value, tupleIndex++);
                    }
                    sb.setLength(0);
                    startQuote = false;
                }
                else if (c == ' ' && !startQuote && sb.length() != 0)
                {
                    if (indices.contains(index++))
                    {
                        Object value;
                        if (tupleIndex == 0)
                        {
                            value = df.parse(sb.toString()).getTime();
                        }
                        else if (tupleIndex == 1)
                        {
                            value = sb.toString();
                            value = ((String)value).substring(0, ((String) value).indexOf(":"));
                        }
                        else
                        {
                            value = sb.toString();
                        }
                        tuple.setField(value, tupleIndex++);
                    }

                    sb.setLength(0);
                }
                else if (c == ' ' && sb.length() == 0)
                {
                    continue;
                }
                else
                {
                    sb.append(c);
                }
            }
            if (indices.contains(index))
            {
                tuple.setField(sb.toString(), tupleIndex);
            }
        }
        catch (Exception e)
        {
            System.out.println(tuple);
            System.out.println(line);
            e.printStackTrace();
            System.exit(0);
        }
        collector.collect(tuple);
    }
}
