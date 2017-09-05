
package com.fusemachines.kinesis.producer;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.fusemachines.kinesis.utility.UUIDGenerator;

/**
 * This the service that publishes data to give Kinesis stream. All the available stream names are also available 
 * through this class. It is better to wrap this service in another kinesis service that is specific to the data 
 * that is to be published. For example if we want to push a 'Person' data to 'people-stream' it is better if we 
 * make 'PersonKinesisService' and autowire this service inside that one. By this way you can implement additional
 * logic unique to operation related to specific stream publish operation inside its own service class.
 * 
 *
 */
@Service
public class KinesisProducerService {

	@Value("${aws.kinesis.person.stream.name}")       
	private String personStreamName;
	@Value("${aws.kinesis.stream2.stream.name}")       
	private String stream2Name;

	@Autowired
	AWSCredentialsProvider awsCredentialsProvider;

	private Logger logger = LoggerFactory.getLogger(KinesisProducerService.class);
	/**
	 * Checks if the stream exists and is active
	 *
	 * @param kinesisClient Amazon Kinesis client instance
	 * @param streamName Name of stream
	 */
	private boolean validateStream(AmazonKinesis kinesisClient, String streamName) {
		try {
			DescribeStreamResult result = kinesisClient.describeStream(streamName);
			if(!"ACTIVE".equals(result.getStreamDescription().getStreamStatus())) {
				logger.error("Stream {} is not active. Please wait a few moments and try again.", streamName);
				return false;
			}
		} catch (ResourceNotFoundException e) {
			logger.error("Stream {} does not exist. Please create it in the console.", streamName);
			return false;
		} catch (Exception e) {
			logger.error("Error found while describing the stream {}", streamName);
			return false;
		}
		return true;
	}

	/**
	 * Uses the Kinesis client to send the stock trade to the given stream.
	 *
	 * @param trade instance representing the stock trade
	 * @param kinesisClient Amazon Kinesis client
	 * @param streamName Name of stream
	 */
	private void send(byte[] data, AmazonKinesis kinesisClient, String streamName) {

		PutRecordRequest putRecord = new PutRecordRequest();
		putRecord.setStreamName(streamName);
		// We use the ticker symbol as the partition key, as explained in the tutorial.
		putRecord.setPartitionKey(UUIDGenerator.getUUID());
		putRecord.setData(ByteBuffer.wrap(data));

		try {
			PutRecordResult result = kinesisClient.putRecord(putRecord);
			logger.info("Data sent to stream {} in shard {}", streamName, result.getShardId());
		} catch (AmazonClientException ex) {
			logger.error("Error sending record to Amazon Kinesis. {}", ex);
		}
	}

	public void sendToStream(String streamName, byte[] data)  {

		AmazonKinesis kinesisClient = AmazonKinesisClientBuilder
				.standard().withRegion(Regions.US_EAST_1)
				.withCredentials(new AWSStaticCredentialsProvider(awsCredentialsProvider.getCredentials()))
				.build();

		// Validate that the stream exists and is active
		validateStream(kinesisClient, streamName);
		send(data, kinesisClient, streamName);

	}

	
	public String getPersonStreamName() {
		return personStreamName;
	}

	public String getStream2Name() {
		return stream2Name;
	}
}
