package com.fusemachines.kinesis.utility;

import java.io.IOException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JacksonByteUtils {

	private static final ObjectMapper JSON = new ObjectMapper();
	static {
		JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	public static byte[] toJsonAsBytes(Object data) {
		try {
			return JSON.writeValueAsBytes(data);
		} catch (IOException e) {
			return new byte[0];
		}
	}

	public static <T> T fromJsonAsBytes(byte[] bytes, Class<T> type) {
		try {
			return JSON.readValue(bytes, type);
		} catch (IOException e) {
			return null;
		}
	}
	
	private JacksonByteUtils( ) { }
}
