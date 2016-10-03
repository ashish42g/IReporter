package tsvReadTime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TSVReadTimeDriver extends Configured implements Tool {

    private static String JOB_NAME = "WRITE-TIME";
    private static int SUCCESS_CODE = 0;

    public TSVReadTimeDriver() {
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new TSVReadTimeDriver(), args));
    }

    public int run(String[] args) throws Exception {

        String INPUT_FOLDER = null;
        String OUTPUT_FOLDER = null;
        if (args.length < 2) {
            throw new Exception("Invalid arguments");
        }
        INPUT_FOLDER = args[0];
        OUTPUT_FOLDER = args[1];

        if ((INPUT_FOLDER.length() <= 0) || (OUTPUT_FOLDER.length() <= 0)) {
            throw new Exception("Invalid arguments");
        }

        Configuration conf = getConf();
        Job job = new Job(conf, JOB_NAME);

        job.setJarByClass(TSVReadTimeDriver.class);
        FileInputFormat.setInputPaths(job, new Path(INPUT_FOLDER));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_FOLDER));

        job.setMapperClass(TSVReadTimeMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(TSVAvgReadTimeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true) ? SUCCESS_CODE : 1;
    }
}