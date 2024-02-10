
import java.util.*;

//aws dependencies

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.Region;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.IamInstanceProfileSpecification;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.*;

//NLP algorithm dependencies
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.tokensregex.types.Tags;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
//import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

import java.io.File;
//json parser dependencies
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class App {

    static AWS aws;

    static String S3bucketName = "gfes";
    static String clientManagerQueueUrl = "manager-client";
    static String clientKey;
    static int n;
    public static void main(String... args) {
        aws = AWS.getInstance();
        clientKey = aws.generateUniqueKey();
        boolean terminate = false;
        if(args[args.length-1].equals("terminate"))
        {
            terminate = true;
            String[] temp = new String[args.length-1];
            for(int i=0; i < temp.length; i++) {
                temp[i] = args[i];
            }
            args = temp;
        }
        if(args.length % 2 == 0)
        {
            System.out.println("wrong arguments");
            System.exit(1);
        }
        //parse args
        HashMap<String,String> inputToKeyMapping = new HashMap<String, String>();
        List<String> inputPaths = new LinkedList<String>();
        List<String> outputPaths = new LinkedList<String>();
        n = Integer.parseInt(args[args.length-1]);
        for(int i = 0; i< args.length-1; i++) {

            if(i < ((args.length-1) / 2)) {
                inputToKeyMapping.put(args[i], aws.generateUniqueKey());
                inputPaths.add(args[i]);
            }
            else
                outputPaths.add(args[i]);
        }
        if(inputPaths.size() != outputPaths.size())
        {
            System.out.println("something is wrong");
            System.exit(1);
        }

        //App logic
        try {
            aws.createBucketIfNotExists(S3bucketName);
            aws.createSQSIfNotExists(clientManagerQueueUrl);
            //Instance managerInstance = getRunningManager();
            for(String inputPath : inputPaths){
                aws.putInBucket(S3bucketName, new File(inputPath), inputToKeyMapping.get(inputPath));
                aws.appendMessageToSQS(clientManagerQueueUrl,"input "+clientKey+" " + inputToKeyMapping.get(inputPath), clientKey);//format: "input <clientKey> <S3 file key>"
                System.out.println("appended input");
            }

            int totalRecieved = 0;
            while(totalRecieved < inputPaths.size()) {
                String[] message = aws.getMessageFromSQS(clientManagerQueueUrl, 0, clientKey);
                
                if(message != null) {
                    System.out.println("received message: "+message[0]);
                }
                if(message != null && message[0].split(" ").length == 3 &&  message[0].split(" ")[0].equals("summary")) {
                    String fileKey = message[0].split(" ")[2];
                    String keyOfInputFile = fileKey; // format of this string is <key>done
                    String outputForThisSummary = null;
                    System.out.println("received summary of: " + keyOfInputFile);
                    for( int i = 0; i<inputPaths.size(); i++) {
                        if(inputToKeyMapping.get(inputPaths.get(i)).equals(keyOfInputFile))
                            outputForThisSummary = outputPaths.get(i);
                    }
                    aws.deleteFromSQS(clientManagerQueueUrl, message[1]);
                    if(outputForThisSummary != null) {
                    aws.getObjectFromBucket(S3bucketName, fileKey+"done", outputForThisSummary);
                    aws.deleteFromBucket(S3bucketName, fileKey+"done");
                    
                    totalRecieved ++;
                    // uncomment the next line to get the html file done
                    //Output.writeOutputToHTMLFile(outputForThisSummary);
                    }
                }else {
                    Thread.sleep(2000);
                }
            }

            if(terminate){
                aws.appendMessageToSQS(clientManagerQueueUrl, "terminate");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Instance createManager() throws Exception {
        String managerEC2name = "Manager";
        //            String ec2Script = "#!/bin/bash\n" +
        //            "echo Hello World\n";
        String ec2Script = "#!/bin/bash\n" +
        "echo Downloading Manager.jar...\n" +
        "mkdir /home/ubuntu/java\n" +
        "cd /home/ubuntu/java\n" +
        "wget https://corretto.aws/downloads/latest/amazon-corretto-21-x64-linux-jdk.tar.gz\n" +
        "sudo tar -xvf amazon-corretto-21-x64-linux-jdk.tar.gz\n" + //get java
        "export PATH=/home/ubuntu/java/amazon-corretto-21.0.2.13.1-linux-x64/bin:$PATH\n" + //set java in path
        "echo 'export PATH=/home/ubuntu/java/amazon-corretto-21.0.2.13.1-linux-x64/bin:$PATH' | tee -a ~/.bashrc\n\n" +//doesnt do anything
        "source ~/.bashrc\n" +
        "wget https://"+S3bucketName+".s3.us-west-2.amazonaws.com/Manager.jar\n" +
        "java -jar Manager.jar "+n+"\n" +
        //                    "wget https://sdjkfhsdjkfhsdjkfhkjbaskjdbasasdasd.s3.us-west-2.amazonaws.com/credentials.zip\n" +
        //                    "sudo apt update\n"+
        //                    "sudo apt install p7zip-full\n"+
        //                    "7z x -p123456 credentials.zip\n" +
        //                    "mkdir -p ~/.aws\n" +
        //                    "mv credentials ~/.aws/credentials\n" +
        //                    "chmod 600 ~/.aws/credentials\n" +
        //                    "rm credentials.zip\n" +

        "echo Running Manager.jar...\n"
        //"java -jar manager.jar\n"
        ;
        String managerInstanceID = aws.createEC2(ec2Script, managerEC2name, 1);
        return aws.getEC2InstanceByTag("Name", managerEC2name).get(0);
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

    private static Instance getRunningManager() throws Exception {
        String managerEC2name = "Manager";
        List<Instance> managers = aws.getEC2InstanceByTag("Name", managerEC2name);
        int runningManagerIndex = 0;
        if(managers.size() == 0)
        {
            createManager();
        }
        else   {
            for(Instance inst : managers) {
                if(inst.state().code() == 16){
                    break;
                }
                runningManagerIndex++;
            }
            if(runningManagerIndex == managers.size()) // not found running manager
                {
                    runningManagerIndex = 0;
                    for(Instance inst : managers) {
                        //if(inst.state().code() == 0 || inst.state().code() == 64 || inst.state().code() == 80){
                            
                            try{
                                Instance newInstance = aws.start(inst); // if instance is still stopping from last run, an exception will be thrown.
                                if(newInstance.state().code() == 16)
                                    break;
                            }catch(Exception e){}
                        //}
                        runningManagerIndex++;
                    }
                }
            }

        
        if(runningManagerIndex == managers.size())
        {
            return createManager();
        }


        return managers.get(runningManagerIndex);
    }
   
}

