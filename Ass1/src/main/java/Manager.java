
import java.util.*;

//aws dependencies
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.Region;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityResponse;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Instance;

//NLP algorithm dependencies

import java.io.File;
//json parser dependencies
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.nio.file.Path;
import java.nio.file.Paths;

public class Manager {

    static String managerWorkerSqsQueueName = "manager-workers";

    static String managerClientSqsQueueName = "manager-client";

    static String S3bucket = "gfes";

    static AWS aws;

    static String credentials;

    static int n;
    
    static int currentWorkers;

    static int currentTasksToProcess;

    static HashMap<String,String> fileKeyToClient = new HashMap<>();

    static HashMap<String,String[]> fileKeyToFragments = new HashMap<>();

    public static void main(String... args) {
        aws = AWS.getInstance();
        aws.putInBucket(S3bucket, "im alive", "manager alive"); //for debugging
       if(args.length < 1){
            System.out.println("not enough arguments");
            System.exit(1);
       }
       System.out.println("running");
       n = Integer.valueOf(args[0]);
       currentWorkers = getAllActiveWorkers().size();
       currentTasksToProcess = 0;
       if(args.length >= 2)
            credentials = args[1];
       aws.createSQSIfNotExists(managerClientSqsQueueName);
       aws.createSQSIfNotExists(managerWorkerSqsQueueName);
       
       boolean keepRunning = true;
       while(keepRunning) {
        System.out.println("tasks being processed: "+currentTasksToProcess);
            String[] message = aws.getMessageFromSQS(managerClientSqsQueueName, 10);
            if(message!=null)
                System.out.println("received message from client: "+message[0]);
            if(message!=null) {
                    if(!message[0].equals("terminate")){
                    String[] messageSegments = message[0].split(" ");
                    String clientKey = "";
                    String fileKey = "";
                    String saveToPath = Path.of("").toAbsolutePath().resolve(message[0]).toString();
                    if(messageSegments[0].equals("input")) {
                        clientKey = messageSegments[1];
                        fileKey = messageSegments[2];
                        fileKeyToClient.put(fileKey, clientKey);
                        boolean success = aws.getObjectFromBucket(S3bucket, fileKey, saveToPath);
                        if(!success){
                            aws.deleteFromSQS(managerClientSqsQueueName, message[1]);
                            try {
                                Thread.sleep(3000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            continue;
                        }
                        int fragments = fragmentizeAndAppendWork(message, fileKey, saveToPath);
                        aws.deleteFromSQS(managerClientSqsQueueName, message[1]);
                        currentTasksToProcess++;
                        initializeWorkerIfNeeded();
                        System.out.println("appended work to workers");
                    }
                } else {
                    for(Instance worker : getAllActiveWorkers()){
                        aws.terminate(worker);
                    }
                    aws.terminate(aws.getEC2InstanceByTag("Name", "Manager").get(0));
                }
                
            }
            message = aws.getMessageFromSQS(managerWorkerSqsQueueName, 0, "finished");
            if(message!=null)
                System.out.println("received message from workers: "+message[0]);
            if(message!=null && message[0].split(" ")[0].equals("finished")) {
                String fileKeyAndFragmentKey = message[0].split(" ")[1];
                String fileKey = fileKeyAndFragmentKey.split("-")[0];
                String clientKey = fileKeyToClient.get(fileKey);
                String summaryFilePath = null;
                try {
                    summaryFilePath = assembleFragment(message, fileKeyAndFragmentKey);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if(summaryFilePath != null){
                    aws.putInBucket(S3bucket, new File(summaryFilePath),fileKey+"done" );
                    aws.appendMessageToSQS(managerClientSqsQueueName, "summary "+fileKeyToClient.get(fileKey)+" "+fileKey, clientKey);
                    currentTasksToProcess--;
                }
            }
            try{
                Thread.sleep(1000);
            }catch(Exception e){}
        }
}
    //if all fragments of a task are assembled, return the path to the summary file, else null
    private static String assembleFragment(String[] message, String fileKeyAndFragmentKey) throws Exception{
        String fileKey = fileKeyAndFragmentKey.split("-")[0];
        String fragmentId = fileKeyAndFragmentKey.split("-")[1];
        String saveFragmentToPath = Path.of("").toAbsolutePath().resolve("summaryOf"+fileKeyAndFragmentKey).toString();
        String saveSummaryToFile = Path.of("").toAbsolutePath().resolve("summaryOf"+fileKey).toString();
        aws.getObjectFromBucket(S3bucket, fileKeyAndFragmentKey+"doneWorker", saveFragmentToPath);
        aws.deleteFromSQS(managerWorkerSqsQueueName, message[1]);
        if(!fileKeyToFragments.containsKey(fileKey)){
                throw new Exception("failed assembleFragment, shouldnt happen");
        }
        fileKeyToFragments.get(fileKey)[Integer.valueOf(fragmentId)-1] = saveFragmentToPath;
        boolean allFragmentsArrived = true;
        for(String str : fileKeyToFragments.get(fileKey)) {
            if(str == null){
                allFragmentsArrived = false;
            }
        }
        if(allFragmentsArrived){
            Output.assembleFiles(fileKeyToFragments.get(fileKey), saveSummaryToFile);
            return saveSummaryToFile;
        }
        //Path.of("").toAbsolutePath().resolve("summaryOf"+fileKey).toString()
        return null;
    }
    //returns the number of fragments created
    private static int fragmentizeAndAppendWork(String[] message, String fileKey, String pathToInputFile) {
        List<Input> fragments = Input.parseFileToInputObjects(pathToInputFile);
        String[] allFragPaths = new String[fragments.size()];
        for(int i=0;i<allFragPaths.length; i++)
            allFragPaths[i] = null;
        fileKeyToFragments.put(fileKey, allFragPaths);
        int index = 1;
        for(Input frag : fragments){
            String localFragmentPath = Path.of("").toAbsolutePath().resolve("frag "+fileKey+"-"+String.valueOf(index)).toString();
            String putInBucketToPath = "task "+fileKey +"-"+String.valueOf(index);
            Input.writeInputObjectToFile(frag, localFragmentPath);
            aws.putInBucket(S3bucket, new File(localFragmentPath), putInBucketToPath);
            aws.appendMessageToSQS(managerWorkerSqsQueueName, putInBucketToPath, "todo");
            index++;
        }
        return fragments.size();
    }

    private static Instance initializeWorkerIfNeeded() {
        if( ((float)currentTasksToProcess) / ((float)n) > ((float)currentWorkers) && currentWorkers <= 4){
                String workerEC2name = "Worker";
            //            String ec2Script = "#!/bin/bash\n" +
            //            "echo Hello World\n";
            String ec2Script = "#!/bin/bash\n" +
            "echo Downloading Worker.jar...\n" +
            "mkdir /home/ubuntu/java\n" +
            "cd /home/ubuntu/java\n" +
            "wget https://corretto.aws/downloads/latest/amazon-corretto-21-x64-linux-jdk.tar.gz\n" +
            "sudo tar -xvf amazon-corretto-21-x64-linux-jdk.tar.gz\n" + //get java
            "export PATH=/home/ubuntu/java/amazon-corretto-21.0.2.13.1-linux-x64/bin:$PATH\n" + //set java in path
            "echo 'export PATH=/home/ubuntu/java/amazon-corretto-21.0.2.13.1-linux-x64/bin:$PATH' | tee -a ~/.bashrc\n\n" +//doesnt do anything
            "source ~/.bashrc\n" +
            "wget https://"+S3bucket+".s3.us-west-2.amazonaws.com/Worker.jar\n" +
            "java -jar Worker.jar Worker\n" +
            "echo Running Worker.jar...\n"
            ;
        //     String ec2Script = "#!/bin/bash\n" +
        // "echo Worker jar running\n" +
        // "echo s3://" + S3bucket + "/" + "Worker.jar" + "\n" +
        // "mkdir WorkerFiles\n" +
        // "aws s3 cp s3://" + S3bucket + "/" + "Worker.jar" + " ./WorkerFiles/" + "Worker.jar" + "\n" +
        // "echo worker copy the jar from s3\n" +
        // "java -jar /WorkerFiles/" + "Worker.jar" + "\n";
            String workerInstanceID = aws.createEC2(ec2Script, workerEC2name, 1);
            currentWorkers ++;
            System.out.println("initialized worker");
            return aws.getEC2InstanceByTag("Name", workerEC2name).get(0);
        }
        return null;
    }

    private static List<Instance> getAllActiveWorkers() {
        List<Instance> total = aws.getEC2InstanceByTag("Name", "Worker");
        List<Instance> ans = new LinkedList<Instance>();
        for(Instance inst: total) {

            if(inst.state().code() == 0 ||inst.state().code() == 16)
                ans.add(inst);
        }
        return ans;
    }

    

}