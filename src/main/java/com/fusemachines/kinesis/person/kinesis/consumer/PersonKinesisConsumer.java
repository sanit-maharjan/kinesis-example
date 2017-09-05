

package com.fusemachines.kinesis.person.kinesis.consumer;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.fusemachines.kinesis.config.ConfigurationUtils;

@Component
public class PersonKinesisConsumer {

	@Value("${aws.kinesis.person.stream.name}")       
	private String personStreamName;
	@Value("${aws.kinesis.person.application.name}")       
	private String personApplicationName;
	
	@Autowired
	private AWSCredentialsProvider awsCredentialProvider;
	
	private Logger logger = LoggerFactory.getLogger(PersonKinesisConsumer.class);
	
	@Async
    public void startConsumer() {

    	logger.info("..........starting stream1 consumer.............");
        String workerId = String.valueOf(UUID.randomUUID());
        KinesisClientLibConfiguration kclConfig =
                new KinesisClientLibConfiguration(personApplicationName, personStreamName, awsCredentialProvider, workerId)
            .withRegionName(Regions.US_EAST_1.getName())
            .withCommonClientConfig(ConfigurationUtils.getClientConfigWithUserAgent());

        IRecordProcessorFactory recordProcessorFactory = new PersonProcessorFactory();

        // Create the KCL worker with the stock trade record processor factory
        final Worker worker = new Worker.Builder()
        		.recordProcessorFactory(recordProcessorFactory)
        		.config(kclConfig)
        		.build();

        int exitCode = 0;
        try {
            worker.run();
        } catch (Exception t) {        	
            logger.error("Caught throwable while processing data. {}", t);
            exitCode = 1;
        }
        System.exit(exitCode);

    }

}
