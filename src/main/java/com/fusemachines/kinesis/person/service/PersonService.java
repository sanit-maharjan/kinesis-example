package com.fusemachines.kinesis.person.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fusemachines.kinesis.person.domain.Person;
import com.fusemachines.kinesis.person.kinesis.PersonKinesisProducer;

@Service
public class PersonService {
	
	private Logger logger = LoggerFactory.getLogger(PersonService.class);

	@Autowired
	private PersonKinesisProducer producer;
	
	public void publishToStream(Person person) {
		producer.publish(person);
	}
	
	public void processPersonFromStream(Person person) {
		logger.info("Person received from steam : {}", person.getName());
	}
}
