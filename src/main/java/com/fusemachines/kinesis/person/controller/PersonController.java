package com.fusemachines.kinesis.person.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.fusemachines.kinesis.person.domain.Person;
import com.fusemachines.kinesis.person.kinesis.PersonKinesisProducer;

@RestController
@RequestMapping("/persons")
public class PersonController {
	
	@Autowired
	private PersonKinesisProducer producer;
	
	@RequestMapping(value = "publish", method = RequestMethod.POST)
	public void publishToStream(@RequestBody Person person) {
		producer.publish(person);		
	}

}
