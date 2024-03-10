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
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class CalculatePMI {

    public static String bucketName;
    public static AWS aws;
    public static String uniqueWord = "43uireoaugibghui4reagf";

    public static class MapperClass extends Mapper<LongWritable, Text, Text , Text> {
        private Text word_1;
        private Text word_2;
        private Text years;
        private LongWritable matchCount;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            try{
            word_1 = new Text();
            word_2 = new Text();
            years = new Text();
            matchCount = new LongWritable();
          
            StringTokenizer itr = new StringTokenizer(value.toString());
            word_1.set(itr.nextToken());
            word_2.set(itr.nextToken());
            if(itr.countTokens() >= 2){
                context.write(word_2, value);
            } else{
                 
                if(!word_2.toString().equals(uniqueWord))
                    context.write(word_2, value);
            }
            }catch(Exception e){
            e.printStackTrace();
            throw new IOException("key: "+key.toString() +" value: "+value.toString()+" the last stacktrace: "+e.getMessage());
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
        double sumPMI = 0.0;
        Configuration config = null;
        
        public void setup(Context context) {
            config = context.getConfiguration();
            config.getStrings("decade");
            decade = config.getStrings("decade")[0];
            N = config.getLong("N", -1);
            sumPMI = config.getDouble("sumPMI", -1);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            Text w1 = new Text();
            Text w2 = new Text();
            List<Text> arr = new ArrayList<Text>();
            long c_w2 = -1;
            for(Text value: values){
                try{
                    StringTokenizer str = new StringTokenizer(value.toString());
                    if(str.countTokens() == 1){
                        context.write(key, value);
                        continue;
                    }
                    w1 = new Text(str.nextToken());
                    w2 = new Text(str.nextToken());
                    if(str.countTokens() == 1){ // c(w1)
                        c_w2 = Long.parseLong(str.nextToken());
                    }
                    else{ // an entire entry
                        arr.add(value);
                    }
                }catch(Exception e){
                    throw new IOException("key: "+key.toString() +" value: "+value.toString()+" the last stacktrace: "+e.getMessage());
                }
            }
            for( Text value : arr){
                try{
                StringTokenizer str = new StringTokenizer(value.toString());
                w1 = new Text(str.nextToken());
                w2 = new Text(str.nextToken());
                Text years = new Text(str.nextToken());
                Text decadeCount = new Text(str.nextToken());
                Text w1_count = new Text(str.nextToken());
                double npmi = npmi(Long.parseLong(decadeCount.toString()), Long.parseLong(w1_count.toString().split(":")[1]), c_w2, N);
                sumPMI += npmi;
                context.write(value, new Text(String.valueOf(npmi)));
                }catch(Exception e){
                    throw new IOException("key: "+key.toString() +" value: "+value.toString()+" the last stacktrace: "+e.getMessage());
                }
            }
            context.getCounter("sumPMI", decade).increment((long)(sumPMI * 1000D));
        }
    }


    //Receives n args: 0 - Step3 1 - inputFolderJoinedW1,2 -inputFolderCountW ,3 - N'sInputFolder,  4 - outputFolder
    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 4 started!");
        aws = AWS.getInstance();
        bucketName = aws.bucketName;
        System.out.println("args: ");
        for(int i=0; i<args.length;i++){
            System.out.print("args["+i+"]"+" : "+args[i] +", ");
        }
        System.out.println("\n");
        String inputFolderJoinedW1 = args[1];
        String inputFolderCountW = args[2];
        String NinputFolder = args[3];
        String outputFolder = args[4];
        
        String decade = "1400's"; // probably doesn't exists, but it's okay.
        
        while(decade != null) {
        String N_string = aws.getObjectFromBucket(bucketName, NinputFolder+"/"+"N_"+decade+".txt");
        if(N_string == null || decade == null){
            System.out.println(N_string +" N_string for decade "+decade);
            decade = getNextDecade(decade);
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
        job.setJarByClass(CalculatePMI.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        //job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        CombineFileInputFormat.addInputPath(job, new Path("s3://"+bucketName+"/"+inputFolderJoinedW1+"/"+decade+"_JoinW1.txt/part-r-00000"));
        CombineFileInputFormat.addInputPath(job, new Path("s3://"+bucketName+"/"+inputFolderCountW+"/"+decade+"_wordCounts.txt/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("s3://"+bucketName+"/"+outputFolder+"/"+decade+"_PMIs.txt"));
        CombineFileInputFormat.setMaxInputSplitSize(job, 500000000);//500MB
        
        job.waitForCompletion(true);
        job.monitorAndPrintJob();
        double sumPMI = ((double)job.getCounters().getGroup("sumPMI").findCounter(decade).getValue()) / 1000.0D;
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
        //System.out.println("calculated sumPMI: "+ CalculatePMI.getN());
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
