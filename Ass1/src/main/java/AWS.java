import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.AWSCredentials;

public class AWS {
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;

    public static String ami = "ami-0c7217cdde317cfec";

    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;

    private static final AWS instance = new AWS();
    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region2).build();
    }

    public static AWS getInstance() {
        return instance;
    }

    public String generateUniqueKey() {
        int x = (int)(Math.random()*10000000);
        return String.valueOf(x); // it will be unique in a very high probability
    }

    public void createSQSIfNotExists(String queueUrl) {
        try {
            sqs.createQueue(CreateQueueRequest
                    .builder()
                    .queueName(queueUrl)
                    // .overrideConfiguration(
                    //         CreateBucketConfiguration.c=builder()
                    //                 .locationConstraint(BucketLocationConstraint.US_WEST_2)
                    //                 .build())
                    .build());
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void appendMessageToSQS(String queueUrl, String message, String clientKey) {
        Map<String,MessageAttributeValue> x = new HashMap();
        x.put(clientKey, MessageAttributeValue.builder().dataType("String").stringValue(clientKey).build());
        SendMessageRequest req = SendMessageRequest.builder().queueUrl(queueUrl).messageAttributes(x).messageBody(message).build();
        SendMessageResponse res = sqs.sendMessage(req);
    }

    public void appendMessageToSQS(String queueUrl, String message) {
        SendMessageRequest req = SendMessageRequest.builder().queueUrl(queueUrl).messageBody(message).build();
        SendMessageResponse res = sqs.sendMessage(req);
    }

    public String[] getMessageFromSQS(String queueUrl, int hideMessageTimeout, String clientKey) {
        String[] ans = new String[2];
        ReceiveMessageRequest req = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .attributeNamesWithStrings(clientKey)
                .visibilityTimeout(hideMessageTimeout)
                .maxNumberOfMessages(1)
                .build();
        ReceiveMessageResponse res = sqs.receiveMessage(req);
        if(res.hasMessages()){
            ans[0] = res.messages().get(0).body();
            ans[1] = res.messages().get(0).receiptHandle();
            return ans;
        }
        else
            return null;
    }

    public String[] getMessageFromSQS(String queueUrl, int hideMessageTimeout) {
        String[] ans = new String[2];
        ReceiveMessageRequest req = ReceiveMessageRequest.builder()
        .queueUrl(queueUrl)
        .visibilityTimeout(hideMessageTimeout)
        .maxNumberOfMessages(1)
        .build();
        ReceiveMessageResponse res = sqs.receiveMessage(req);
        if(res.hasMessages()){
            ans[0] = res.messages().get(0).body();
            ans[1] = res.messages().get(0).receiptHandle();
            return ans;
        }
        else
            return null;
    }

    public void delayMessageInvisibility(String queueUrl, String receipt, int timeOut) {
        sqs.changeMessageVisibility(ChangeMessageVisibilityRequest.builder().queueUrl(queueUrl).receiptHandle(receipt)
        .visibilityTimeout(timeOut).build());
    }


    public void deleteFromSQS(String queueUrl, String receipt){
        DeleteMessageRequest req = DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(receipt).build();
        sqs.deleteMessage(req);
    }

    public void createBucketIfNotExists(String bucketName) {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .objectOwnership(ObjectOwnership.BUCKET_OWNER_PREFERRED)
                    // .acl(BucketCannedACL.)
                    
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                    
                                    .build())
                    .build());
                    
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            s3.putPublicAccessBlock(PutPublicAccessBlockRequest.builder()
                    .bucket(bucketName)
                    .publicAccessBlockConfiguration(PublicAccessBlockConfiguration.builder()
                    .blockPublicAcls(false)
                    .blockPublicPolicy(false)
                    .ignorePublicAcls(false)
                    .restrictPublicBuckets(false).build())
                    .build());
            s3.putBucketAcl(PutBucketAclRequest.builder().bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ_WRITE).build());
            this.putInBucket(bucketName, new File(Path.of("").toAbsolutePath().resolve("Manager.jar").toString()),"Manager.jar");
            this.putInBucket(bucketName, new File(Path.of("").toAbsolutePath().resolve("Worker.jar").toString()),"Worker.jar");
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void putInBucket(String bucketName, File content, String fileKey) {
        s3.putObject(PutObjectRequest.builder().bucket(bucketName).key(fileKey).acl(ObjectCannedACL.PUBLIC_READ_WRITE).build(),content.toPath());
    }

    public void putInBucket(String bucketName, String content, String fileKey) {
        s3.putObject(PutObjectRequest.builder().bucket(bucketName).key(fileKey).acl(ObjectCannedACL.PUBLIC_READ_WRITE).build(),RequestBody.fromString(content));
    }

    public boolean getObjectFromBucket(String bucketName, String keyName, String path) {
        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
            byte[] data = objectBytes.asByteArray();
            // Write the data to a local file.
            File myFile = new File(path);
            OutputStream os = new FileOutputStream(myFile);
            os.write(data);
            System.out.println("Successfully obtained bytes from an S3 object");
            os.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
            //System.exit(1);

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            return false;
            //System.exit(1);
        }
        return true;
    }

    public String getObjectFromBucket(String bucketName, String keyName) {
        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
            byte[] data = objectBytes.asByteArray();
            return new String(data, StandardCharsets.UTF_8);
            // // Write the data to a local file.
            // File myFile = new File(path);
            // OutputStream os = new FileOutputStream(myFile);
            // os.write(data);
            // System.out.println("Successfully obtained bytes from an S3 object");
            // os.close();

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }

    public void deleteFromBucket(String bucketName, String fileKey) {
        s3.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(fileKey).build());
    }

    public void deleteBucket(String bucketName) {
        s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build());
    }
    public void purgeSQS(String queueUrl){
        PurgeQueueRequest pqRequest = PurgeQueueRequest.builder()
                .queueUrl(queueUrl)
                .build();
        sqs.purgeQueue(pqRequest);
    }
    // EC2
    public String createEC2Worker(String script, String tagName, int numberOfInstances) {
        Ec2Client ec2 = Ec2Client.builder().region(region2).build();

        // CreateSecurityGroupRequest createSecurityGroupRequest = CreateSecurityGroupRequest.builder()
        // .groupName("BaeldungSecurityGroup")
        // .description("Baeldung Security Group")
        // .build();
    
        // ec2.createSecurityGroup(createSecurityGroupRequest);
        
        RunInstancesRequest runRequest = (RunInstancesRequest) RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_LARGE)
                .imageId(ami)
                .maxCount(numberOfInstances)
                .minCount(1)
                .keyName("vockey")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString((script).getBytes()))
                .build();


        RunInstancesResponse response = ec2.runInstances(runRequest);

        String securityGroupId = response.instances().get(0).securityGroups().get(0).groupId();
        // Adding inbound rule to the security group allowing SSH from any source
        //very useful to check the ec2 remotely
        ec2.authorizeSecurityGroupIngress(AuthorizeSecurityGroupIngressRequest.builder()
                .groupId(securityGroupId)
                .ipProtocol("tcp")
                .fromPort(22)
                .toPort(22)
                .build());
        String instanceId = response.instances().get(0).instanceId();

        software.amazon.awssdk.services.ec2.model.Tag tag = Tag.builder()
                .key("Name")
                .value(tagName)
                .build();

        CreateTagsRequest tagRequest = (CreateTagsRequest) CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "[DEBUG] Successfully started EC2 instance %s based on AMI %s\n",
                    instanceId, ami);

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            System.exit(1);
        }
        return instanceId;
    }
    // EC2
    public String createEC2(String script, String tagName, int numberOfInstances) {
        Ec2Client ec2 = Ec2Client.builder().region(region2).build();

        // CreateSecurityGroupRequest createSecurityGroupRequest = CreateSecurityGroupRequest.builder()
        // .groupName("BaeldungSecurityGroup")
        // .description("Baeldung Security Group")
        // .build();

        // ec2.createSecurityGroup(createSecurityGroupRequest);

        RunInstancesRequest runRequest = (RunInstancesRequest) RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_MEDIUM)
                .imageId(ami)
                .maxCount(numberOfInstances)
                .minCount(1)
                .keyName("vockey")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString((script).getBytes()))
                .build();


        RunInstancesResponse response = ec2.runInstances(runRequest);

        String securityGroupId = response.instances().get(0).securityGroups().get(0).groupId();
        // Adding inbound rule to the security group allowing SSH from any source
        //very useful to check the ec2 remotely
        ec2.authorizeSecurityGroupIngress(AuthorizeSecurityGroupIngressRequest.builder()
                .groupId(securityGroupId)
                .ipProtocol("tcp")
                .fromPort(22)
                .toPort(22)
                .build());
        String instanceId = response.instances().get(0).instanceId();

        software.amazon.awssdk.services.ec2.model.Tag tag = Tag.builder()
                .key("Name")
                .value(tagName)
                .build();

        CreateTagsRequest tagRequest = (CreateTagsRequest) CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "[DEBUG] Successfully started EC2 instance %s based on AMI %s\n",
                    instanceId, ami);

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            System.exit(1);
        }
        return instanceId;
    }

    public void describeEC2Instances(){

        String nextToken = null;

        try {

            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(12).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);

                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        System.out.println("Instance Id is " + instance.instanceId());
                        System.out.println("Image id is "+  instance.imageId());
                        System.out.println("Instance type is "+  instance.instanceType());
                        System.out.println("Instance state name is "+  instance.state().name());
                        System.out.println("monitoring information is "+  instance.monitoring().state());

                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public List<Instance> getEC2InstanceByTag(String tag, String value) {
        List<Instance> ans = new LinkedList<Instance>();
        String nextToken = null;

        try {

            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                        .filters(Filter.builder().name("tag:"+tag).values(value).build()).maxResults(12)
                        .nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);

                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        ans.add(instance);
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }

        return ans;
    }

    public Instance start(Instance instance) {
        StartInstancesRequest req = StartInstancesRequest.builder().instanceIds(instance.instanceId()).build();
        StartInstancesResponse res = ec2.startInstances(req);
        ec2.waiter().waitUntilInstanceRunning(DescribeInstancesRequest.builder().instanceIds(instance.instanceId()).build());
        //ec2.waiter().waitUntilInstanceStatusOk(DescribeInstanceStatusRequest.builder().instanceIds(instance.instanceId()).build());
        DescribeInstancesResponse res2 = ec2.describeInstances(DescribeInstancesRequest.builder().instanceIds(instance.instanceId()).build());
        return res2.reservations().get(0).instances().get(0);
    }

    public void stop(Instance instance) {
        StopInstancesRequest req = StopInstancesRequest.builder().instanceIds(instance.instanceId()).build();
        StopInstancesResponse res = ec2.stopInstances(req);
    }

    public void terminate(Instance instance) {
        TerminateInstancesRequest req = TerminateInstancesRequest.builder().instanceIds(instance.instanceId()).build();
        TerminateInstancesResponse res = ec2.terminateInstances(req);
    }

}
