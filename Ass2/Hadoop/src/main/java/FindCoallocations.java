import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
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
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
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
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class FindCoallocations {

    public static String bucketName;
    public static AWS aws;

    public static class MapperClass extends Mapper<LongWritable, Text, DoubleWritable , Text> {
        String decade;
        double sumNPMI;
        double minNPMI;
        double minRelNPMI;

        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            decade = conf.getStrings("decade",decade)[0];
            sumNPMI = conf.getDouble("SumNPMI", -1);
            minNPMI = conf.getDouble("minNPMI", -1);
            minRelNPMI = conf.getDouble("minRelNPMI", -1);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
           String[] record =value.toString().split("\t");
           double npmi = Double.parseDouble(record[6]);
           if((npmi >= minNPMI || (npmi/sumNPMI) >= minRelNPMI) && npmi < 1.0){
                Text coallocation = new Text(record[0] +"\t"+record[1]);
                context.write(new DoubleWritable((-1) * npmi), coallocation); // switched in order to sort by NPMI value
           }
        }
    }

    public static class PartitionerClass extends Partitioner<DoubleWritable, Text> {
        
        @Override
        public int getPartition(DoubleWritable key, Text value, int numPartitions) {
            return 1; // all pairs to be processed by one reducer.
        }
    }

    public static class ReducerClass extends Reducer<DoubleWritable,Text,Text,Text> {

        public void setup(Context context) {
            
        }
        
        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            for (Text val: values){
                context.write(val, new Text("NPMI:"+((-1) *key.get()) )); // switch order back
            }
        }
    }


    //Receives n args: 0 - Step5 ,1- minNPMI, 2- minRelPMI ,3 - inputFolderName, 4 - inputSumPMIFolder ,5 - outputFolder
    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 5 started!");
        aws = AWS.getInstance();
        bucketName = aws.bucketName;
        double minNPMI = Double.parseDouble(args[1]);
        double minRelPMI = Double.parseDouble(args[2]);
        String inputFolder = args[3];
        String inputSumNPMIFolder = args[4];
        String outputFolder = args[5];
        for(int i =0; i<= 5; i++)
            System.out.println("args["+i+"]: "+args[i]);
        String decade = "1490's"; // probably doesn't exists, but it's okay.
        
        while(decade != null) {
        String sumNPMI = aws.getObjectFromBucket(bucketName, inputSumNPMIFolder+"/sumPMI_"+decade+".txt");
        if(sumNPMI == null || decade == null){
            decade = getNextDecade(decade);
            continue;
        }
        double sumNpmi = Double.parseDouble(sumNPMI);
        System.out.println("decade: "+decade);
        
        Configuration conf = new Configuration();
        conf.setQuietMode(false);
        conf.setStrings("decade",decade);
        conf.setDouble("SumNPMI", sumNpmi);
        conf.setDouble("minNPMI", minNPMI);
        conf.setDouble("minRelNPMI", minRelPMI);
        Job job = Job.getInstance(conf, "Find Coallocations" + decade);
        job.setJarByClass(FindCoallocations.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputDirRecursive(job, true); 
        FileInputFormat.addInputPath(job, new Path("s3://"+bucketName+"/"+inputFolder +"/"+decade+"_PMIs.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://"+bucketName+"/"+outputFolder+"/"+decade+"_Coallocations"));
        CombineFileInputFormat.setMaxInputSplitSize(job, 500000000);//500MB
        
        job.waitForCompletion(true);
        job.monitorAndPrintJob();
        decade = getNextDecade(decade);
    }
}

    public static String getNextDecade(String decade){
        String cur =  decade.substring(0, 4);
        int newDecade = Integer.parseInt(cur) + 10;
        if(newDecade == 2030)
            return null;
        else
            return String.valueOf(newDecade) + "'s";
    }
}
