
package com.fusemachines.kinesis.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

/**
 * Provides utilities for retrieving credentials to talk to AWS
 */
@Configuration
public class AwsCredentialsConfiguration {

	@Value("${aws.access.key}")
	private String awsAccessKey;
	@Value("${aws.secret.key}")
	private String awsSecretKey;
	
	@Bean
    public AWSCredentialsProvider getCredentialsProvider() {
        AWSCredentialsProvider credentialsProvider = null;
        try {
        	AWSCredentials basicAwsCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
        	credentialsProvider = new AWSStaticCredentialsProvider(basicAwsCredentials);
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file." + e);
        }
        return credentialsProvider;
    }

}
