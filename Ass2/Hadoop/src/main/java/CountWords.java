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
import org.apache.hadoop.mapred.Task.Counter;
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class CountWords {

    public static String bucketName;
    public static AWS aws;
    public static boolean debugMode = true;
    public static String uniqueWord = "43uireoaugibghui4reagf"; //This word doesn't exist in the corpus (hopefully).
    public static String decade;
    public static long N = 1;

    public static void setN (long val){
        CountWords.N = val;
    }
    public static long getN (){
        return CountWords.N;
    }
    public static String getDecade(){
        return decade;
    }
    public static void setDecade(String dec) {
       CountWords.decade = dec;
    }
    
    public static class MapperClassW1 extends Mapper<LongWritable, Text, Text , LongWritable> {
        
        private Text word_1;
        private Text word_2;
        private Text years;
        private LongWritable matchCount;

        public String decade;
        
        @Override
        public void setup(Context context) {
            Configuration config = context.getConfiguration();
            decade = config.getStrings("decade")[0];
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            word_1 = new Text();
            word_2 = new Text();
            years = new Text();
            matchCount = new LongWritable();
          
            StringTokenizer itr = new StringTokenizer(value.toString());
            word_1.set(itr.nextToken());
            word_2.set(itr.nextToken());
            years.set(itr.nextToken());
            long longMatchCount = Long.parseLong(itr.nextToken());
            matchCount.set(longMatchCount); 
            
            if(years.toString().equals(decade)){
                Text countN = new Text(uniqueWord);

                context.write(countN, new LongWritable(longMatchCount*2)); // used for counting N (number of words)
                context.write (new Text(word_1.toString()+"\t"+uniqueWord), matchCount); // used for claculating c(w1)
                context.write (new Text(uniqueWord+"\t"+word_2.toString()), matchCount);
                //throw new IOException("exception at mapper. key: "+key.toString()+" value: "+value.toString() +" word_1: "+word_1.toString());
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
           
            return (key.toString().hashCode() % numPartitions);
        }
    }

    public static class ReducerClassW1 extends Reducer<Text,LongWritable,Text,LongWritable> {

        public String decade;

        public void setup(Context context) {
            Configuration config = context.getConfiguration();
            decade = config.getStrings("decade")[0];
        }

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            if((key.toString()).equals(uniqueWord)) // count N
            {
                long sum = 0;
                for (LongWritable value : values) {
                    sum += value.get();
                }
                //context.write(key, new Text(Long.toString(sum)));
                context.getCounter("N", decade).increment(sum);
            }
            else  // count w1 ( value is full entry)
            {
                String[] splitKey = key.toString().split("\t");
                long sum = 0;
                for (LongWritable value : values) {
                        try{
                            sum += value.get();
                            
                        }catch (Exception e){
                            throw new IOException ("the key: "+key.toString()+" the value: "+value+ " sum: "+sum );
                        }
                    
                }
                context.write(key, new LongWritable(sum));  
            }
           
        }
    }

    //Receives n args: 0 - Step1 , 1 - decade ("all" for all decades) , 2 - inputFileName1, ....... n-1 inputFileName(n-2) , n outputFileName
    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        aws = AWS.getInstance();
        bucketName = aws.bucketName;
        int inputs = args.length;
        boolean allDecades = false;
        System.out.println("args: ");
        for(int i=0; i<args.length;i++){
            System.out.print("args["+i+"]"+" : "+args[i] +", ");
        }
        System.out.println("\n");
        String[] inputFolder = new String[args.length-3];
        String outputFileKey = args[args.length-1];
        for(int i=2; i<args.length - 1;i++){
           inputFolder[i-2] = args[i];
        }
        if(args[1].equals("all")){
            allDecades = true;
            CountWords.setDecade("1400's"); // starting decade
        }
        else
        {
            CountWords.setDecade(args[1]);
            allDecades = false;
        }

        do {
        System.out.println("decade: "+CountWords.getDecade());
        for(int i=0; i<inputFolder.length;i++){
            System.out.println("input: "+inputFolder[i]);
         }
         System.out.println("output: "+outputFileKey);

        
        Configuration conf = new Configuration();
        conf.setQuietMode(false);
        conf.setStrings("decade",CountWords.getDecade());
        
        Job job = Job.getInstance(conf, "Count w1 w2" + decade);
        job.setJarByClass(CountWords.class);
        job.setMapperClass(MapperClassW1.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClassW1.class);
        job.setReducerClass(ReducerClassW1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        for(int i=0; i<inputFolder.length; i++){
            FileInputFormat.addInputPath(job, new Path("s3://"+bucketName+"/"+inputFolder[i]+"/"+decade+"/part-r-00000"));
        }
        FileOutputFormat.setOutputPath(job, new Path("s3://"+bucketName+"/"+outputFileKey+"/"+decade+"_wordCounts.txt"));
        

        job.waitForCompletion(true);
        job.monitorAndPrintJob();
        CountWords.setN(job.getCounters().getGroup("N").findCounter(decade).getValue());
        String fileName = Paths.get("").toAbsolutePath().resolve("N_"+decade+".txt").toString();
        try {
            FileWriter fileWriter = new FileWriter(fileName);
            fileWriter.write(CountWords.getN() + "");
            fileWriter.close();
            System.out.println("String has been written to file");
        } catch (IOException e) {
            e.printStackTrace();
        }
        aws.putInBucket(bucketName, Paths.get("").toAbsolutePath().resolve(fileName).toFile(), outputFileKey+"/N_"+decade+".txt"); 
        System.out.println("calculated N: "+ CountWords.getN());
        CountWords.setDecade(getNextDecade(decade));
        } while(allDecades && decade != null);

//        For n_grams S3 files.
//        Note: This is English version and you should change the path to the relevant one
//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data"));
        
    }

    public static String getNextDecade(String decade){
        String cur =  decade.substring(0, 4);
        int newDecade = Integer.parseInt(cur) + 10;
        if(newDecade == 2030)
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
