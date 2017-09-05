
package com.fusemachines.kinesis.person.kinesis.consumer;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import com.fusemachines.kinesis.ApplicationContextManager;
import com.fusemachines.kinesis.person.domain.Person;
import com.fusemachines.kinesis.person.service.PersonService;
import com.fusemachines.kinesis.utility.JacksonByteUtils;

/**
 * Processes records retrieved from person steram.
 *
 */
public class PersonProcessor implements IRecordProcessor {

	private Logger logger = LoggerFactory.getLogger(PersonProcessor.class);
	private String kinesisShardId;

	// Checkpointing interval
	private static final long CHECKPOINT_INTERVAL_MILLIS = 5000; // 5 seconds
	private long nextCheckpointTimeInMillis;

	@Override
	public void initialize(InitializationInput initializationInput) {
		logger.info("Initializing record processor for shard: {}", initializationInput.getShardId());
		this.kinesisShardId = initializationInput.getShardId();
		nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;		
	}

	@Override
	public void processRecords(ProcessRecordsInput processRecordsInput) {
		for (Record record : processRecordsInput.getRecords()) {
			// process record
			processRecord(record);
		}

		// Checkpoint once every checkpoint interval
		if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
			checkpoint(processRecordsInput.getCheckpointer());
			nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
		}

	}

	@Override
	public void shutdown(ShutdownInput shutdownInput) {
		logger.info("Shutting down record processor for shard: {}", kinesisShardId);
		// Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
		if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
			checkpoint(shutdownInput.getCheckpointer());
		}

	}

	private void processRecord(Record record) {
		ApplicationContext ctx = ApplicationContextManager.getAppContext();
		PersonService service = ctx.getBean(PersonService.class);	
		
		Person person = JacksonByteUtils.fromJsonAsBytes(record.getData().array(), Person.class);
		service.processPersonFromStream(person);
	}


	private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
		logger.info("Checkpointing shard: {}", kinesisShardId);
		try {
			checkpointer.checkpoint();
		} catch (ShutdownException se) {
			// Ignore checkpoint if the processor instance has been shutdown (fail over).
			logger.info("Caught shutdown exception, skipping checkpoint.", se);
		} catch (ThrottlingException e) {
			// Skip checkpoint when throttled. In practice, consider a backoff and retry policy.
			logger.error("Caught throttling exception, skipping checkpoint.", e);
		} catch (InvalidStateException e) {
			// This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
			logger.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
		}
	}


}
