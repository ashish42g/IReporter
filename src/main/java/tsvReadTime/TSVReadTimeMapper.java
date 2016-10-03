package tsvReadTime;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TSVReadTimeMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    long startTime = 0L;
    long minTime = Long.MAX_VALUE;
    long maxTime = Long.MIN_VALUE;
    long totalRecord = 0L;
    long totalDuration = 0L;


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        long duration;
        totalRecord++;


        if (startTime == 0) {
            startTime = System.nanoTime();
        } else {
            duration = System.nanoTime() - startTime;
            startTime = System.nanoTime();

            if (minTime > duration) {
                minTime = duration;
            }

            if (maxTime < duration) {
                maxTime = duration;
            }

            totalDuration += duration;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String value = minTime + "\t" + maxTime + "\t" + totalRecord + "\t" + totalDuration;
        context.write(new IntWritable(1), new Text(value));
    }
}
