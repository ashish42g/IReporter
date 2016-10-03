package parquetReadTime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.Log;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.example.ExampleInputFormat;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

public class ParquetReadTimeDriver extends Configured implements Tool {

    private static final Log LOG = Log.getLog(ParquetReadTimeDriver.class);

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new ParquetReadTimeDriver(), args));
    }

    public int run(String[] args) throws Exception {

        if (args != null && args.length < 2) {
            throw new Exception("Invalid arguments");
        }

        String inputFile = args[0];
        String outputFile = args[1];

        Configuration conf = getConf();

        Path parquetFilePath = new Path(inputFile);
        Path outputPath = new Path(outputFile);

        LOG.info("Getting schema from " + parquetFilePath);
        ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, parquetFilePath);
        MessageType schema = readFooter.getFileMetaData().getSchema();
        GroupWriteSupport.setSchema(schema, getConf());

        LOG.info(schema);

        Job job = new Job(conf, getClass().getName());
        job.setJarByClass(getClass());

        FileInputFormat.setInputPaths(job, parquetFilePath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setInputFormatClass(ExampleInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(ParquetReadTimeMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ParquetAvgReadTimeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
