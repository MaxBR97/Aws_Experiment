import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.StringTokenizer;

public class FilterStopWords {

    public static String bucketName;
    public static AWS aws;
    public static boolean debugMode = true;

    public static class MapperClass extends Mapper<LongWritable, Text, Text , LongWritable> {
        private static LongWritable one = new LongWritable(1);
        

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
           
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            return 0;
        }
    }

    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
          
        }
    }


    //Receives n args: 0 - Step1 1 - inputFileName1, 2 - inputFileName2, ....... n-1 inputFileName(n-1) , n outputFileName
    public static void main(String[] args) throws Exception {
    }

}
