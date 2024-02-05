
import java.io.File;
import java.nio.file.Path;
import java.util.*;

import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityResponse;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;


public class Worker {
    static String manageWorkerSqsQueueName = "manager-workers";
    static String S3bucket = "gfes";
    static int visibilityTimeout = 10; // seconds to make message invisible in queue but still present.
    static String saveTaskToFile = "task";
    static String[] toHandle;
    static AWS aws;
    public static Object lock = new Object();
    public static void main(String... args) {
        aws = AWS.getInstance();
        saveTaskToFile =  Path.of("").toAbsolutePath().resolve(saveTaskToFile).toString();
        activateTimingThread();
        while(true){
            String[] message = aws.getMessageFromSQS(manageWorkerSqsQueueName, visibilityTimeout, "todo");
            if(message!=null)
                System.out.println(message[0]);
            if(message != null && message[0].split(" ")[0].equals("task")) {
                toHandle = message;
                synchronized(lock){lock.notify();}
                System.out.println("received task");
                String fileKey = message[0].split(" ")[1];
                aws.getObjectFromBucket(S3bucket,"task "+fileKey, saveTaskToFile + fileKey);
                List<Input> inputs = Input.parseFileToInputObjects(saveTaskToFile + fileKey);
                ReviewMapper reviewMapper = new ReviewMapper();
                Output outputManager = new Output();
                for(Input in : inputs){
                    for(Review review : in.getReviews()) {
                        outputManager.appendProcessedReview(reviewMapper.process(review));
                    }
                }
                outputManager.writeOutputToJSONFile(Path.of("").toAbsolutePath().resolve("finishedTask.json").toString());
                aws.putInBucket(S3bucket, new File(Path.of("").toAbsolutePath().resolve("finishedTask.json").toString()),fileKey+"doneWorker");
                aws.appendMessageToSQS(manageWorkerSqsQueueName, "finished "+fileKey, "finished");
                aws.deleteFromSQS(manageWorkerSqsQueueName, message[1]);
                toHandle = null;
                System.out.println("appended finished");
            }
        }
    }
    private static Thread activateTimingThread() {
        Thread t = new Thread(new Runnable() {
            public void run() {
                    while(true){
                        try {
                            synchronized(lock){lock.wait();}
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    System.out.println("started counting time");
                    long startTime = System.currentTimeMillis();
                    long timeElapsed = 0;
                        while(toHandle != null) {
                                timeElapsed = System.currentTimeMillis() - startTime;
                                if(timeElapsed >= (visibilityTimeout * 1000)/2)
                                    {
                                        aws.delayMessageInvisibility(manageWorkerSqsQueueName, toHandle[1], visibilityTimeout);
                                        timeElapsed = 0;
                                        startTime = System.currentTimeMillis();
                                        System.out.println("delayed sqs message invisibility");
                                    }
                            try{
                                Thread.sleep(100);
                            } catch(Exception e){}
                        }
                    }
            }
        });
        t.start();
        return t;
    }
    
}
