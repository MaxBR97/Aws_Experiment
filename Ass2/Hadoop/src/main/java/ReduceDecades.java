import org.apache.hadoop.conf.Configuration;
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.StringTokenizer;

public class ReduceDecades {

    public static String bucketName;
    public static AWS aws;
    public static boolean debugMode = true;

    public static class MapperClass extends Mapper<LongWritable, Text, Text , Text> {
        
        private Text word_1;
        private Text word_2;
        private Text year;
        private LongWritable matchCount;
        private String decade;

        public void setup(Context context) {
            Configuration config = context.getConfiguration();
            decade = config.getStrings("decade")[0];
        }
        

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
           // try{
                    word_1 = new Text();
                    word_2 = new Text();
                    year = new Text();
                    matchCount = new LongWritable();

                    StringTokenizer itr = new StringTokenizer(value.toString());
                    if(checkValidity(value.toString())){
                        word_1.set(itr.nextToken());
                        word_2.set(itr.nextToken());
                        String yearRecord = itr.nextToken();
                        year.set((yearRecord.substring(0, 3)).concat("0's"));
                        matchCount.set(Long.parseLong(itr.nextToken()));
                        
                        Text ans = new Text();
                        ans.set(word_1.toString() +"\t"+ word_2.toString() +"\t"+ year.toString());
                        if(year.toString().equals(decade))
                            context.write(ans, new Text(String.valueOf(matchCount.get())));
                    }
            // }catch (Exception e){
            //     throw new IOException("key: "+key.toString() + " value: "+value.toString()+" message: "+e.getMessage() + " trace: "+e.getStackTrace().toString());
            // }
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

    public static class PartitionerClass extends Partitioner<Text, Text> {
        
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.toString().hashCode()) % numPartitions;
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (Text value : values) {
                long matchCount = Long.parseLong(value.toString());
                sum += matchCount;
            }
            key = new Text(key.toString().replace(' ', '\t'));
            context.write(key, new Text(String.valueOf(sum)));
        }
    }


    //Receives n args: 0 - Step1 ,1 - starting decade ,2 - inputFileName1, 3 - inputFileName2, ....... n-1 inputFileName(n-2) , n outputFileFolder
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

        for(int i=0; i<inputFileKey.length;i++){
            System.out.println("input: "+inputFileKey[i]);
         }
         System.out.println("out: "+outputFileKey);
         
        String decade = args[1];
        Job job = null;
        while(decade != null){
        Configuration conf = new Configuration();
        conf.setQuietMode(false);
        conf.setStrings("decade", decade);
        
        job = Job.getInstance(conf, "Reduce Decades");
        job.setJarByClass(ReduceDecades.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        //job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        

//        For n_grams S3 files.
//        Note: This is English version and you should change the path to the relevant one
    //    job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
    //    job.setInputFormatClass(SequenceFileInputFormat.class);
       //TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data"));
        TextInputFormat.setInputDirRecursive(job, true); 
        for(int i=0; i<inputFileKey.length; i++){
            TextInputFormat.addInputPath(job, new Path(inputFileKey[i]));
            //FileInputFormat.addInputPath(job, new Path("s3://"+bucketName+"/"+inputFileKey[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path("s3://"+bucketName+"/"+outputFileKey+"/"+decade));
        
        job.submit();
        
        job.monitorAndPrintJob();
        decade = getNextDecade(decade);
        }
        while(!job.isComplete()){
            Thread.sleep(2000);
        }
        System.exit(0);
        
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
