import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.TextOutputFormat;
// import org.apache.hadoop.mapred.InputFormat;
// import org.apache.hadoop.mapred.SplitLocationInfo;
// import org.apache.hadoop.mapred.TextInputFormat;
// import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class FilterStopWords {

    public static String bucketName;
    public static AWS aws;

    public static class MapperClass extends Mapper<LongWritable, Text, Text , LongWritable> {
        private static LongWritable one = new LongWritable(1);
        private Set<String> wordSet = new HashSet<>();
        private Text word_1;
        private Text word_2;
        private Text decade;
        private LongWritable matchCount;
        private static String bucketName;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Load the list of words to filter out
            Configuration conf = context.getConfiguration();
            bucketName = conf.get("bucketName");
            String fileName =  conf.get("filterList");
            String filePath = "s3://" + bucketName + "/" + fileName;
            System.out.println(filePath);
            try {
                FileSystem fs = FileSystem.get(new URI(filePath), conf);
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(filePath))))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        wordSet.add(line);
                    }
                }
            } catch (Exception  e) {

                throw new IOException("ERROR PATH" + filePath );

            }

        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {

            word_1 = new Text();
            word_2 = new Text();
            decade = new Text();
            matchCount = new LongWritable();

            StringTokenizer itr = new StringTokenizer(value.toString());
            if(checkValidity(value.toString())) {
                word_1.set(itr.nextToken());
                word_2.set(itr.nextToken());
                decade.set(itr.nextToken());
                matchCount.set(Long.parseLong(itr.nextToken()));

                Text ans = new Text();
                ans.set(word_1.toString() + "\t" + word_2.toString() + "\t" + decade.toString());
                if (!wordSet.contains(word_1.toString()) && !wordSet.contains(word_2.toString())) {
                    context.write(ans, matchCount);
                }
            }

        }

        private boolean checkValidity(String record){
            try{
                StringTokenizer itr = new StringTokenizer(record);
                if(itr.countTokens() >= 4){
                    itr.nextToken();
                    itr.nextToken();
                    String year = itr.nextToken();
                    if(year.length() == 4 && Integer.parseInt(year) > 0){ // validate year format
                        if(Long.parseLong(itr.nextToken())>=0){
                            return true;
                        }

                    }
                }
                return false;
            } catch (Exception e){
                return false;
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {

        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            return ((key.toString().hashCode() & Integer.MAX_VALUE) % numPartitions);

        }
    }

    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            for (LongWritable value : values) {
                context.write(key, value);
            }
        }
    }


    //Receives n args: 0 - Step1 1 - inputFileName1, 2 - inputFileName2, ....... n-1 inputFileName(n-1) , n outputFileName
    public static void main(String[] args) throws Exception {
        //type <inputPath>* <outputPath> <stopwordfile>
        System.out.println("[DEBUG] STEP 0 started!");
        aws = AWS.getInstance();
        bucketName = aws.bucketName;
        int inputs = args.length;
        System.out.println("args: ");
        for(int i=0; i<args.length;i++){
            System.out.print("args["+i+"]"+" : "+args[i] +", ");
        }
        System.out.println("\n");
        String[] inputFileKey = new String[args.length-3];
        String outputFileKey = args[args.length-2];
        String stopwordsFileKey = args[args.length-1];
        for(int i=1; i<args.length - 2;i++){
            inputFileKey[i-1] = args[i];
        }
        System.out.println("input file size : "+inputFileKey.length);

        for(int i=0; i<inputFileKey.length;i++){
            System.out.println("input: "+inputFileKey[i]);
        }
        System.out.println("out: "+outputFileKey);
        System.out.println("stop word file: "+stopwordsFileKey);


        Configuration conf = new Configuration();
        conf.setStrings("filterList", stopwordsFileKey);
        conf.setStrings("bucketName", bucketName);

        conf.setQuietMode(false);
        Job filterStopWords = Job.getInstance(conf, "Filter Stop");

        Job job = Job.getInstance(conf, "Filter Stop");
        job.setJarByClass(FilterStopWords.class);
        job.setMapperClass(FilterStopWords.MapperClass.class);
        job.setPartitionerClass(FilterStopWords.PartitionerClass.class);
        job.setCombinerClass(FilterStopWords.ReducerClass.class);
        job.setReducerClass(FilterStopWords.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        for(int i=0; i<inputFileKey.length; i++){
            TextInputFormat.addInputPath(job, new Path(inputFileKey[i]));
            //FileInputFormat.addInputPath(job, new Path("s3://"+bucketName+"/"+inputFileKey[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path("s3://"+bucketName+"/"+outputFileKey));


        job.waitForCompletion(true);

        job.monitorAndPrintJob();

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
