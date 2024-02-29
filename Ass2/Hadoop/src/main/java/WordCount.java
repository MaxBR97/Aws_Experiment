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

public class WordCount {

    public static String bucketName;
    public static AWS aws;
    public static boolean debugMode = true;


    public static class SplitClass extends InputSplit{

        @Override
        public long getLength() throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'getLength'");
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'getLocations'");
        }}
    public static class RecordReaderClass extends RecordReader{

            private InputSplit split;
            private TaskAttemptContext context;
            @Override
            public void initialize(InputSplit split, TaskAttemptContext context)
                    throws IOException, InterruptedException {
                this.split = split;
                this.context = context;
            }

            @Override
            public boolean nextKeyValue() {
            
                return false; //unimplemeneted
            }

            @Override
            public Object getCurrentKey() throws IOException, InterruptedException {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Unimplemented method 'getCurrentKey'");
            }

            @Override
            public Object getCurrentValue() throws IOException, InterruptedException {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Unimplemented method 'getCurrentValue'");
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Unimplemented method 'getProgress'");
            }

            @Override
            public void close() throws IOException {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Unimplemented method 'close'");
            }}
    public static class MapperClass extends Mapper<LongWritable, Text, Text , LongWritable> {
        
        private Text word_1;
        private Text word_2;
        private Text decade;
        private LongWritable matchCount;
        

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            debug("map:  key - "+key.toString() +" , value - "+value.toString()+" , context - "+context.toString() + " , split - "+context.getInputSplit());
            word_1 = new Text();
            word_2 = new Text();
            decade = new Text();
            matchCount = new LongWritable();

            StringTokenizer itr = new StringTokenizer(value.toString());
            word_1.set(itr.nextToken());
            word_2.set(itr.nextToken());
            decade.set((itr.nextToken().substring(0, 3)).concat("0's"));
            matchCount.set(Long.parseLong(itr.nextToken()));
            
            Text ans = new Text();
            ans.set(word_1.toString() +" "+ word_2.toString() +" "+ decade.toString());
           
            context.write(ans, matchCount);
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            debug("key - " + key.toString() + " value - " + value.toString() + " numPartitions - " + numPartitions + "returns - " + (key.hashCode() % numPartitions) );
            String[] line = key.toString().split(" ");
            return (line[0] +" "+ line[1]).hashCode() % numPartitions;
        }
    }

    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            System.out.println("check");
            debug("reduce:  key - "+key.toString() +" , context - "+context.toString() +" status - " + context.getStatus());
            
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
            debug(" status - " + context.getStatus());
        }
    }


    //Receives n args: 0 - Step1 1 - inputFileName1, 2 - inputFileName2, ....... n-1 inputFileName(n-1) , n outputFileName
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
        String[] inputFileKey = new String[args.length-2];
        String outputFileKey = args[args.length-1];
        for(int i=1; i<args.length - 1;i++){
           inputFileKey[i-1] = args[i];
        }

        for(int i=0; i<inputFileKey.length;i++){
            System.out.println("input: "+inputFileKey[i]);
         }
         System.out.println("out: "+outputFileKey);

        
        Configuration conf = new Configuration();
        conf.setQuietMode(false);
        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        

//        For n_grams S3 files.
//        Note: This is English version and you should change the path to the relevant one
//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data"));
        
        for(int i=0; i<inputFileKey.length; i++){
            FileInputFormat.addInputPath(job, new Path("s3://"+bucketName+"/"+inputFileKey[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path("s3://"+bucketName+"/"+outputFileKey));
        
        job.waitForCompletion(true);
        
        job.monitorAndPrintJob();
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void debug(String message){
        if(debugMode)
            System.out.println(message);
    }

    public static double npmi(String word1, String word2){
        double nominator = pmi(word1, word2);
        double denominator = (-1) * Math.log(p(word1, word2));
        return (nominator/denominator);
    }

    public static double pmi(String w1, String w2){
        return 1;
    }
    public static double p(String w1, String w2){
       return 1;
    }

}




 // File myFile = new File(Paths.get("").toAbsolutePath().resolve("testtt").toString());
            
            // OutputStream os = new FileOutputStream(myFile);
            // os.write(("map:  key - "+key.toString() +" , value - "+value.toString()+" , context - "+context.toString() + " , split - "+context.getInputSplit()).getBytes());
            // os.close();
            // aws.putInBucket(bucketName, new File(Paths.get("").toAbsolutePath().resolve("testtt").toString()), "testtt");