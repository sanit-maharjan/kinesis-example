package com.fusemachines.kinesis.person.kinesis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fusemachines.kinesis.person.domain.Person;
import com.fusemachines.kinesis.producer.KinesisProducerService;
import com.fusemachines.kinesis.utility.JacksonByteUtils;

@Component
public class PersonKinesisProducer {

	@Autowired
	private KinesisProducerService service;
	
	public void publish(Person person) {
		service.sendToStream(service.getPersonStreamName(), JacksonByteUtils.toJsonAsBytes(person));
	}

}
