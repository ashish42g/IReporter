package parquetReadTime;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;

import java.io.IOException;

public class ParquetReadTimeMapper extends Mapper<LongWritable, Group, IntWritable, Text> {

    long startRecTime = 0l;
    long minRecTime = Long.MAX_VALUE;
    long maxRecTime = Long.MIN_VALUE;
    long recCnt = 0;
    long tot_duration = 0l;


    @Override
    protected void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {

        long duration;
        recCnt++;

        if ((startRecTime == 0)) {
            startRecTime = System.nanoTime();
        } else {

            duration = System.nanoTime() - startRecTime;
            startRecTime = System.nanoTime();

            if (minRecTime > duration) {
                minRecTime = duration;
            }

            if (maxRecTime < duration) {
                maxRecTime = duration;
            }

            tot_duration = tot_duration + duration;
        }
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String value = minRecTime + "\t" + maxRecTime + "\t" + recCnt + "\t" + tot_duration;
        context.write(new IntWritable(1), new Text(value));
    }
}
