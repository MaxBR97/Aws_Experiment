import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

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

    public static int numberOfInstances = 1;

    public static void main(String[]args){
        aws = AWS.getInstance();
        bucketName = aws.bucketName;
        emr = aws.emr;
        
        String inputFile = "arbix.txt";
        String outputFile = "output";

        aws.createBucketIfNotExists(bucketName);
        aws.putInBucket(bucketName, Paths.get("").toAbsolutePath().resolve(inputFile).toFile(), inputFile);
        //aws.putInBucket(bucketName, Paths.get("").toAbsolutePath().resolve("WordCount.jar").toFile(), "WordCount.jar");

        // Step 1
        HadoopJarStepConfig step1 = new HadoopJarStepConfig()
                .withJar("s3://"+bucketName+"/WordCount.jar")
                .withMainClass("Step1")
                .withArgs(inputFile, outputFile);

        StepConfig stepConfig1 = new StepConfig()
                .withName("Step1")
                .withHadoopJarStep(step1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //Job flow`
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(numberOfInstances)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.9.2")
                .withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        System.out.println("Set steps");
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Map reduce project")
                .withInstances(instances)
                .withSteps(stepConfig1)
                .withLogUri("s3://"+bucketName+"")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

        // while(!aws.getObjectFromBucket(bucketName, outputFile, Path.of(".").toAbsolutePath().resolve(outputFile).toString())){
        //         try{
        //                 Thread.sleep(3000);
        //         } catch(Exception e){}
        // }
        // aws.deleteFromBucket(bucketName, outputFile);
        // System.out.println("recieved output file: "+outputFile);
    }
}