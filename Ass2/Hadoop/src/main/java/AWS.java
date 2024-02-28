import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.PublicAccessBlockConfiguration;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.SetPublicAccessBlockRequest;
import com.amazonaws.services.s3.model.ownership.ObjectOwnership;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.BucketLocationConstraint;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutBucketAclRequest;
import software.amazon.awssdk.services.s3.model.PutPublicAccessBlockRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;


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


    public static AWSCredentialsProvider credentialsProvider;
    public static S3Client s3;
    public static AmazonEC2 ec2;
    public static AmazonElasticMapReduce emr;

    public static String bucketName = "mykey161";

    public static String region = "us-east-1";

    private static final AWS instance = new AWS();

    private AWS() {
        credentialsProvider = new ProfileCredentialsProvider();
        System.out.println("[INFO] Connecting to aws");
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(region)
                .build();
        s3 = S3Client.builder().region(software.amazon.awssdk.regions.Region.US_WEST_2).build();

        emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(region)
                .build();
               
    }

    public static AWS getInstance() {
        return instance;
    }

   public void createBucketIfNotExists(String bucketName) {
        try {
            s3.createBucket(software.amazon.awssdk.services.s3.model.CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .objectOwnership(software.amazon.awssdk.services.s3.model.ObjectOwnership.BUCKET_OWNER_PREFERRED)
                    // .acl(BucketCannedACL.)
                    
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                   // .locationConstraint(BucketLocationConstraint.US_WEST_2)
                    
                                    .build())
                    .build());
                    
            s3.waiter().waitUntilBucketExists(software.amazon.awssdk.services.s3.model.HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            s3.putPublicAccessBlock(PutPublicAccessBlockRequest.builder()
                    .bucket(bucketName)
                    .publicAccessBlockConfiguration(software.amazon.awssdk.services.s3.model.PublicAccessBlockConfiguration
                    .builder()
                    .blockPublicAcls(false)
                    .blockPublicPolicy(false)
                    .ignorePublicAcls(false)
                    .restrictPublicBuckets(false).build())
                    .build());
            s3.putBucketAcl(PutBucketAclRequest.builder().bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ_WRITE).build());
        } catch (S3Exception e) {
            //System.out.println(e.getMessage());
        }
    }

    public void putInBucket(String bucketName, File content, String fileKey) {
        s3.putObject(software.amazon.awssdk.services.s3.model.PutObjectRequest.builder().bucket(bucketName).key(fileKey).acl(ObjectCannedACL.PUBLIC_READ_WRITE).build(),content.toPath());
    }

    public boolean getObjectFromBucket(String bucketName, String keyName, String path) {
        try {
            software.amazon.awssdk.services.s3.model.GetObjectRequest objectRequest = software.amazon.awssdk.services.s3.model.GetObjectRequest
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
            software.amazon.awssdk.services.s3.model.GetObjectRequest objectRequest = software.amazon.awssdk.services.s3.model.GetObjectRequest
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
        s3.deleteObject(software.amazon.awssdk.services.s3.model.DeleteObjectRequest.builder().bucket(bucketName).key(fileKey).build());
    }

    public void deleteBucket(String bucketName) {
      
        s3.deleteBucket(software.amazon.awssdk.services.s3.model.DeleteBucketRequest.builder().bucket(bucketName).build());
    }

}
