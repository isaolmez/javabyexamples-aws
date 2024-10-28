package com.javabyexamples.aws.s3.temporarycredentials;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;

public class MakingRequestsWithIAMTempCredentials {

    private final String clientRegion = "us-east-1";
    private final AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder.standard()
      .withRegion(clientRegion)
      .build();

    public void operateWithTemporaryCredentials(String bucketName) {
        String roleARN = "arn:aws:iam::518833209733:role/roleForS3ToUser";
        String roleSessionName = "roleSessionName";

        try {
            // Obtain credentials for the IAM role. Note that you cannot assume the role of an AWS root account;
            // Amazon S3 will deny access. You must use credentials for an IAM user or an IAM role.
            AssumeRoleRequest roleRequest = new AssumeRoleRequest()
              .withRoleArn(roleARN)
              .withRoleSessionName(roleSessionName);
            AssumeRoleResult roleResponse = stsClient.assumeRole(roleRequest);
            Credentials sessionCredentials = roleResponse.getCredentials();

            // Create a BasicSessionCredentials object that contains the credentials you just retrieved.
            BasicSessionCredentials awsCredentials = new BasicSessionCredentials(
              sessionCredentials.getAccessKeyId(),
              sessionCredentials.getSecretAccessKey(),
              sessionCredentials.getSessionToken());

            System.out.printf("Access key: %s. Secret key: %s%n", sessionCredentials.getAccessKeyId(),
              sessionCredentials.getSecretAccessKey());

            // Provide temporary security credentials so that the Amazon S3 client 
            // can send authenticated requests to Amazon S3. You create the client
            // using the sessionCredentials object.
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
              .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
              .withRegion(clientRegion)
              .build();

            // Verify that assuming the role worked and the permissions are set correctly
            // by getting a set of object keys from the bucket.
            ObjectListing objects = s3Client.listObjects(bucketName);
            System.out.println("No. of Objects: " + objects.getObjectSummaries().size());
        } catch (SdkClientException e) {
            System.out.println("Error occurred: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        final MakingRequestsWithIAMTempCredentials tempCredentials = new MakingRequestsWithIAMTempCredentials();
        tempCredentials.operateWithTemporaryCredentials("javabyexamples");
    }
}
