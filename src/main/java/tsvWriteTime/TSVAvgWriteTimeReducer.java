package tsvWriteTime;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TSVAvgWriteTimeReducer extends Reducer<Text, Text, Text, Text> {

    long minTime = 0L;
    long maxTime = 0L;
    long totalDuration = 0L;
    int totalRecord = 0;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        if (key != null && key.toString().equalsIgnoreCase("1")) {
            for (Text value : values) {
                if (value != null) {
                    String[] tokens = value.toString().split("/t");
                    if (tokens.length > 3) {
                        setMinTime(Long.valueOf(tokens[0]));
                        setMaxTime(Long.valueOf(tokens[1]));
                        totalDuration += Long.valueOf(tokens[2]);
                        totalRecord += Long.valueOf(tokens[3]);
                    }
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        long avgDuration = totalDuration / totalRecord;

        context.write(new Text("TSV DATA WRITE TIME ANALYSIS IN NANO SECONDS \n"),
                new Text("\n MIN TIME : " + minTime +
                        "\n MAX TIME : " + maxTime +
                        "\n TOTAL DURATION : " + totalDuration +
                        "\n TOTAL RECORDS : " + totalRecord +
                        "\n AVG TIME : " + avgDuration));
    }

    private void setMaxTime(long duration) {
        if (maxTime < duration) {
            maxTime = duration;
        }
    }

    private void setMinTime(long duration) {
        if (minTime == 0) {
            minTime = duration;
        } else if (minTime > duration) {
            minTime = duration;
        }
    }
}
