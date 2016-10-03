package parquetReadTime;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ParquetAvgReadTimeReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long min = 0l;
        long max = 0l;
        long recCount = 0l;
        long totTime = 0l;
        long avg;

        for (Text val : values) {
            String[] fields = val.toString().split("\t");
            long min_val = Long.parseLong(fields[0]);
            long max_val = Long.parseLong(fields[1]);

            if (min == 0)
                min = min_val;
            else if (min > min_val)
                min = min_val;

            if (max < max_val)
                max = max_val;

            recCount = recCount + Long.parseLong(fields[2]);
            totTime = totTime + Long.parseLong(fields[3]);

        }

        avg = totTime / recCount;
        context.write(new Text("Avro summary: min:" + min + " ns\t max:" + max + " ns \tavg:" + avg + " ns\ttot:" + totTime + " ns\ttxns:" + recCount), NullWritable.get());
    }
}
