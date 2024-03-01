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
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.List;
import java.util.StringTokenizer;

public class FindCoallocations {

    public static String bucketName;
    public static AWS aws;
    public static boolean debugMode = true;
    public static String uniqueWord = "43uireoaugibghui4reagf"; //This word doesn't exist in the corpus (hopefully).
    public static String decade;
    public static long N = 1;

    public static void setN (long val){
        FindCoallocations.N = val;
    }
    public static long getN (){
        return FindCoallocations.N;
    }
    public static String getDecade(){
        return decade;
    }
    public static void setDecade(String dec) {
       FindCoallocations.decade = dec;
    }
    
    public static class MapperClassW1 extends Mapper<LongWritable, Text, Text , Text> {
        
        private Text word_1;
        private Text word_2;
        private Text years;
        private Text matchCount;

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
            matchCount = new Text();
          
            StringTokenizer itr = new StringTokenizer(value.toString());
            word_1.set(itr.nextToken());
            word_2.set(itr.nextToken());
            years.set(itr.nextToken());
            matchCount.set(itr.nextToken()); 
            
            if(years.toString().equals(decade)){
                Text countN = new Text(uniqueWord);
            
                context.write(countN, matchCount); // used for counting N (number of words)
                context.write (word_1, value); // used for claculating c(w1)
                //throw new IOException("exception at mapper. key: "+key.toString()+" value: "+value.toString() +" word_1: "+word_1.toString());
            }
        }
    }

    public static class MapperClassW2 extends Mapper<LongWritable, Text, Text , Text> {
        
        private Text word_1;
        private Text word_2;
        private Text years;
        private Text matchCount;
        
        public String decade;

        public void setup(Context context) {
            Configuration config = context.getConfiguration();
            decade = config.getStrings("decade")[0];
        }
   
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            try{
            word_1 = new Text();
            word_2 = new Text();
            years = new Text();
            matchCount = new Text();
            StringTokenizer check  = new StringTokenizer(value.toString());
            
           
                StringTokenizer itr = new StringTokenizer(value.toString());
                word_1.set(itr.nextToken());
                word_2.set(itr.nextToken());
                years.set(itr.nextToken());
                matchCount.set(itr.nextToken());
            
                context.write (word_2, value);
            
        } catch(Exception e){
            throw new IOException("key: "+key.toString() + " value: "+value.toString());
        }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
           
            return (key.toString().hashCode() % numPartitions);
        }
    }

    public static class ReducerClassW1 extends Reducer<Text,Text,Text,Text> {

        public String decade;

        public void setup(Context context) {
            Configuration config = context.getConfiguration();
            decade = config.getStrings("decade")[0];
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            if((key.toString()).equals(uniqueWord)) // count N
            {
                long sum = 0;
                for (Text value : values) {
                    sum += Long.parseLong(value.toString());
                }
                //context.write(key, new Text(Long.toString(sum)));
                context.getCounter(Counter.COMBINE_INPUT_RECORDS).increment(sum);
            }
            else  // count w1 ( value is full entry)
            {
               
                long sum = 0;
                for (Text value : values) {
                    try{
                    StringTokenizer itr = new StringTokenizer(value.toString());
                    itr.nextToken();
                    itr.nextToken();
                    itr.nextToken();
                    sum += Long.parseLong(itr.nextToken());
                    } catch(Exception e){
                        throw new IOException ("the key: "+key.toString()+" the value: "+value.toString()+ " sum: "+sum);
                    }
               }
                for (Text value : values) {
                    context.write(value, new Text("w1:"+Long.toString(sum)));
                }
              
            }
           
        }
    }

    public static class ReducerClassW2 extends Reducer<Text,Text,Text,Text> {
        public String decade;
        public long N;

        public void setup(Context context) {
            Configuration config = context.getConfiguration();
            decade = config.getStrings("decade")[0];
            N = config.getLong("N", 0);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
                
               long sum = 0;
                
                for (Text value : values) {
                    try{
                        StringTokenizer itr = new StringTokenizer(value.toString());
                        itr.nextToken();
                        itr.nextToken();
                        itr.nextToken();
                        sum += Long.parseLong(itr.nextToken());
                    } catch(Exception e){
                        throw new IOException ("the key: "+key.toString()+" the value: "+value.toString()+ " sum: "+sum);
                    }
                }
                for (Text value : values) {
                    try{
                    StringTokenizer itr = new StringTokenizer(value.toString());
                    Text w1 = new Text(itr.nextToken());
                    Text w2 = new Text(itr.nextToken());
                    Text cDecade = new Text(itr.nextToken());
                    Text matchCount = new Text(itr.nextToken());
                    long w1_w2_count_parsed = Long.parseLong(matchCount.toString());
                    Text w1_count = new Text(itr.nextToken());
                    long w1_count_parsed= Long.parseLong(w1_count.toString().split(":")[1]);
                    Text w2_count = new Text("w2:"+Long.toString(sum));
                    long w2_count_parsed = Long.parseLong(w2_count.toString().split(":")[1]);
                    Text N_count = new Text("N:"+Long.toString(N));
                    
                    double npmi = npmi(w1_w2_count_parsed ,w1_count_parsed,w2_count_parsed, N);
                    Text npmi_value = new Text("npmi:"+npmi);
                    context.write(value, new Text(w2_count.toString() + "\t" + npmi_value.toString() ));
                    } catch(Exception e){
                        throw new IOException ("the key: "+key.toString()+" the value: "+value.toString()+ " sum: "+sum);
                    }
                }
           
        }
    }


    //Receives n args: 0 - Step1 , 1 - decade , 2 - inputFileName1, ....... n-1 inputFileName(n-2) , n outputFileName
    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        aws = AWS.getInstance();
        bucketName = aws.bucketName;
        int inputs = args.length;
        System.out.println("args: ");
        for(int i=0; i<args.length;i++){
            System.out.print("args["+i+"]"+" : "+args[i] +", ");
        }
        System.out.println("\n");
        String[] inputFileKey = new String[args.length-3];
        String outputFileKey = args[args.length-1];
        for(int i=2; i<args.length - 1;i++){
           inputFileKey[i-2] = args[i];
        }
        FindCoallocations.setDecade(args[1]);
        System.out.println("decade: "+FindCoallocations.getDecade());
        for(int i=0; i<inputFileKey.length;i++){
            System.out.println("input: "+inputFileKey[i]);
         }
         System.out.println("output: "+outputFileKey);

        
        Configuration conf = new Configuration();
        conf.setQuietMode(false);
        conf.setStrings("decade",FindCoallocations.getDecade());
        
        Job job = Job.getInstance(conf, "Join w1 count");
        job.setJarByClass(FindCoallocations.class);
        job.setMapperClass(MapperClassW1.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClassW1.class);
        job.setReducerClass(ReducerClassW1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        for(int i=0; i<inputFileKey.length; i++){
            FileInputFormat.addInputPath(job, new Path("s3://"+bucketName+"/"+inputFileKey[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path("s3://"+bucketName+"/"+outputFileKey+"_"+decade+"_w1"));
        job.waitForCompletion(true);
        job.monitorAndPrintJob();
        FindCoallocations.setN(job.getCounters().findCounter(Counter.COMBINE_INPUT_RECORDS).getValue());
        conf.setLong("N", FindCoallocations.getN());
        System.out.println("calculated N: "+ FindCoallocations.getN());
        // Job filterStopWords = Job.getInstance(conf, "join w2 count");
        // filterStopWords.setJarByClass(FindCoallocations.class);
        // filterStopWords.setMapperClass(MapperClassW2.class);
        // filterStopWords.setPartitionerClass(PartitionerClass.class);
        // filterStopWords.setCombinerClass(ReducerClassW2.class);
        // filterStopWords.setReducerClass(ReducerClassW2.class);
        // filterStopWords.setMapOutputKeyClass(Text.class);
        // filterStopWords.setMapOutputValueClass(Text.class);
        // filterStopWords.setOutputKeyClass(Text.class);
        // filterStopWords.setOutputValueClass(Text.class);

        // FileInputFormat.addInputPath(filterStopWords, new Path("s3://"+bucketName+"/"+outputFileKey+"_"+decade+"_w1"));
        // FileOutputFormat.setOutputPath(filterStopWords, new Path("s3://"+bucketName+"/"+outputFileKey));
        // filterStopWords.waitForCompletion(true);

//        For n_grams S3 files.
//        Note: This is English version and you should change the path to the relevant one
//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data"));
        
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
