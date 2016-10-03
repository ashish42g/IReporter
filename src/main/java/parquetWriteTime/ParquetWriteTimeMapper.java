package parquetWriteTime;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;

import java.io.IOException;

public class ParquetWriteTimeMapper extends Mapper<LongWritable, Group, IntWritable, Text> {

    long minTime = 0L;
    long maxTime = 0L;
    long totalDuration = 0L;
    int totalRecord = 0;

    @Override
    protected void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
        long startTime = System.nanoTime();

        context.write(new IntWritable(1), new Text(""));

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
        context.write(new IntWritable(1), new Text(minTime + "/t" + maxTime + "/t" + totalDuration + "/t" + totalRecord));
    }
}
