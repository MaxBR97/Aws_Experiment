import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.StringTokenizer;

import org.apache.hadoop.mapred.Task.Counter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

public class App {

    
    public static AmazonElasticMapReduce emr;
    public static AWS aws ;
    public static String bucketName ;

    public static int numberOfInstances = 8;

    public static void main(String[]args){

        aws = AWS.getInstance();
        bucketName = aws.bucketName;
        emr = aws.emr;
        if (args.length != 3) {
            System.out.println("error Usage: classname  minPmi relMinPmi");
            return;
        }

        String minPmi = args[1];
        String relMinPmi = args[2];
        //step 0
        HadoopJarStepConfig step0 = new HadoopJarStepConfig()
                .withJar("s3://"+bucketName+"/FilterStopWords.jar")
                .withMainClass("Step0")
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data" , "step0_output","heb-stopwords.txt");
        // "s3://"+bucketName+"/"+"example_of_2gram_input1" ,"s3://"+bucketName+"/"+"example_of_2gram_input2"
        //"s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/2gram/data"
        StepConfig stepConfig0 = new StepConfig()
                .withName("Step0")
                .withHadoopJarStep(step0)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        // Step 1 - map reduce
        HadoopJarStepConfig step1 = new HadoopJarStepConfig()
                .withJar("s3://"+bucketName+"/ReduceDecades.jar")
                .withMainClass("Step1")
                .withArgs("1500's","step0_output" , "step1_output");
                // "s3://"+bucketName+"/"+"example_of_2gram_input1" ,"s3://"+bucketName+"/"+"example_of_2gram_input2" 
                //"s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/2gram/data"
        StepConfig stepConfig1 = new StepConfig()
                .withName("Step1")
                .withHadoopJarStep(step1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        HadoopJarStepConfig step2 = new HadoopJarStepConfig()
                .withJar("s3://"+bucketName+"/CountWords.jar")
                .withMainClass("Step2")
                .withArgs("1500's", "step1_output" , "step2_output");

        StepConfig stepConfig2 = new StepConfig()
                .withName("Step2")
                .withHadoopJarStep(step2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        
        HadoopJarStepConfig step3 = new HadoopJarStepConfig()
                .withJar("s3://"+bucketName+"/JoinW1.jar")
                .withMainClass("Step3")
                .withArgs("step1_output" , "step2_output" , "step3_output");

        StepConfig stepConfig3 = new StepConfig()
                .withName("Step3")
                .withHadoopJarStep(step3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig step4 = new HadoopJarStepConfig()
                .withJar("s3://"+bucketName+"/CalculatePMI.jar")
                .withMainClass("Step4")
                .withArgs("step3_output","step2_output","step2_output", "step4_output");

        StepConfig stepConfig4 = new StepConfig()
                .withName("Step4")
                .withHadoopJarStep(step4)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        
        HadoopJarStepConfig step5 = new HadoopJarStepConfig()
                .withJar("s3://"+bucketName+"/FindCoallocations.jar")
                .withMainClass("Step5")
                .withArgs(minPmi,relMinPmi,"step4_output", "step4_output", "step5_output - FINAL");

        StepConfig stepConfig5 = new StepConfig()
                .withName("Step5")
                .withHadoopJarStep(step5)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        //Job flow`
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(numberOfInstances)
                .withMasterInstanceType(InstanceType.M4Xlarge.toString())
                .withSlaveInstanceType(InstanceType.M4Xlarge.toString())
                .withHadoopVersion("2.9.2")
                .withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        System.out.println("Set steps");
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Map reduce project")
                .withInstances(instances)
                .withSteps(stepConfig0,stepConfig1, stepConfig2, stepConfig3 , stepConfig4, stepConfig5  )
                .withLogUri("s3://"+bucketName+"")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
