package com.fusemachines.kinesis.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;

@Configuration
public class KinesisConfiguration {

	@Autowired
	AWSCredentialsProvider awsCredentialsProvider;
	
	@Bean
	public AmazonKinesis getAmazonKinesisClient() {
		return AmazonKinesisClientBuilder
	                .standard()
	                .withRegion(Regions.US_EAST_1)
	                .withCredentials(new AWSStaticCredentialsProvider(awsCredentialsProvider.getCredentials()))
	                .build();		 
	}
}
