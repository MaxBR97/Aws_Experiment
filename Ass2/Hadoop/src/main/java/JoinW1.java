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
import org.apache.hadoop.mapred.Task.Counter;
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
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class JoinW1 {

    public static String bucketName;
    public static AWS aws;

    public static class MapperClass extends Mapper<LongWritable, Text, Text , Text> {
        private Text word_1;
        private Text word_2;
        private Text years;
        private LongWritable matchCount;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            word_1 = new Text();
            word_2 = new Text();
            years = new Text();
            matchCount = new LongWritable();
          
            StringTokenizer itr = new StringTokenizer(value.toString());
            word_1.set(itr.nextToken());
            word_2.set(itr.nextToken());
            if(itr.countTokens() == 2){
                years.set(itr.nextToken());
                matchCount.set(Long.parseLong(itr.nextToken())); 
                context.write(word_1, value);
            } else{
                matchCount.set(Long.parseLong(itr.nextToken())); 
                if(!word_1.equals(CountWords.uniqueWord))
                    context.write(word_1, value);
            }
            
            
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            
            return key.toString().hashCode() % numPartitions;
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        String decade;
        long N;
        double sumPMI;
        
        public void setup(Context context) {
            Configuration config = context.getConfiguration();
            decade = config.getStrings("decade")[0];
            N = config.getLong("N", -1);
            sumPMI = config.getDouble("sumPMI", -1);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            Text w1 = new Text();
            Text w2 = new Text();
            List<Text> arr = new ArrayList<Text>();
            long c_w1 = 0;
            for(Text value: values){
                
                StringTokenizer str = new StringTokenizer(value.toString());
                w1 = new Text(str.nextToken());
                w2 = new Text(str.nextToken());
                if(str.countTokens() == 1){ // c(w1)
                    c_w1 = Long.parseLong(str.nextToken());
                }
                else{ // an entire entry
                    arr.add(value);
                }
            }
            for( Text value : arr){
                Text ans = new Text("w1:"+c_w1);
                context.write(value, ans);
            }
        }
    }


    //Receives n args: 0 - Step3 1 - inputFolder, 2 - outputFolder
    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");
        aws = AWS.getInstance();
        bucketName = aws.bucketName;
        System.out.println("args: ");
        for(int i=0; i<args.length;i++){
            System.out.print("args["+i+"]"+" : "+args[i] +", ");
        }
        System.out.println("\n");
        String inputFolder = args[1];
        String outputFolder = args[2];
        
        String decade = "1400's"; // probably doesn't exists, but it's okay.
        String N_string = aws.getObjectFromBucket(bucketName, inputFolder+"/"+"N_"+decade+".txt");
        while(decade != null) {
        if(N_string == null){
            System.out.println(N_string +" N_string is null");
            continue;
        }
        long N = Long.parseLong(N_string);
        System.out.println("decade: "+decade);
        
        Configuration conf = new Configuration();
        conf.setQuietMode(false);
        conf.setStrings("decade",decade);
        conf.setLong("N", N);
        conf.setDouble("sumPMI", 0.0D);
        Job job = Job.getInstance(conf, "Calculate PMIs " + decade);
        job.setJarByClass(JoinW1.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path("s3://"+bucketName+"/"+inputFolder+"/"+decade+"_wordCounts.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://"+bucketName+"/"+outputFolder+"/"+decade+"_PMIs.txt"));
        
        job.waitForCompletion(true);
        job.monitorAndPrintJob();
        double sumPMI = conf.getDouble("sumPMI", -1);
        String fileName = Paths.get("").toAbsolutePath().resolve("sumPMI_"+decade+".txt").toString();
        try {
            FileWriter fileWriter = new FileWriter(fileName);
            fileWriter.write(sumPMI + "");
            fileWriter.close();
            System.out.println("String has been written to file");
        } catch (IOException e) {
            e.printStackTrace();
        }
        aws.putInBucket(bucketName, Paths.get("").toAbsolutePath().resolve(fileName).toFile(), outputFolder+"/sumPMI_"+decade+".txt"); 
        //System.out.println("calculated sumPMI: "+ JoinW1.getN());
         decade = getNextDecade(decade);
        } 

    }

    public static String getNextDecade(String decade){
        String cur =  decade.substring(0, 4);
        int newDecade = Integer.parseInt(cur) + 10;
        if(newDecade == 2040)
            return null;
        else
            return String.valueOf(newDecade) + "'s";
    }

    public static double npmi(long w1_w2 ,long w1,long w2, long n){
        double nominator = pmi(w1_w2, w1, w2, n);
        double denominator = (-1) * Math.log(p(w1_w2, n));
        return (nominator/denominator);
    }

    public static double pmi(long w1_w2,long w1, long w2, long n){
        return Math.log(w1_w2) + Math.log(n) - Math.log(w1) - Math.log(w2);
    }

    public static double p(long w1_w2, long n){
        return w1_w2/n;
    }

}
