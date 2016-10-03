package tsvWriteTime;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TSVWriteTimeMapper extends Mapper<LongWritable, Text, Text, Text> {

    long minTime = 0L;
    long maxTime = 0L;
    long totalDuration = 0L;
    int totalRecord = 0;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        long startTime = System.nanoTime();

        context.write(new Text(key.toString()), value);

        long endTime = System.nanoTime();
        long duration = endTime - startTime;

        setMinTime(duration);
        setMaxTime(duration);

        totalDuration += duration;
        totalRecord++;
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

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("1"), new Text(minTime + "/t" + maxTime + "/t" + totalDuration + "/t" + totalRecord));
    }
}