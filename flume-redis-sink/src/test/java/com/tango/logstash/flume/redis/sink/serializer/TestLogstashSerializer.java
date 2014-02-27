/**
 *  Copyright 2014 TangoMe Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.tango.logstash.flume.redis.sink.serializer;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.Test;

import com.tango.logstash.flume.redis.core.LogstashConstant;
import com.tango.logstash.flume.redis.core.LogstashEvent;
import com.tango.logstash.flume.redis.sink.serializer.LogstashSerializer;
import com.tango.logstash.flume.redis.sink.serializer.RedisSerializerException;
import com.tango.logstash.flume.redis.sink.serializer.Serializer;

public class TestLogstashSerializer {

	@Test(expected = IllegalArgumentException.class)
	public void nullEventTest() throws RedisSerializerException {

		Serializer serializer = new LogstashSerializer();
		serializer.serialize(null);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void nullLogstashEventTest() throws RedisSerializerException {
		
		LogstashSerializer serializer = new LogstashSerializer();
		serializer.convertToLogstashEvent(null);

	}

	/**
	 * Verifies that standard headers and body are parsed correctly
	 * @throws RedisSerializerException
	 */
	@Test
	public void parsingDefaultEventTest() throws RedisSerializerException {

		LogstashSerializer serializer = new LogstashSerializer();

		byte[] body = new byte[] { '1', '2', '3', '4', '5' };
		String testHost = "testhost";
		String sourcePath = "/my/source/path";
		String type = "mytype";
		String source = "my source";
		List<String> tags = new ArrayList<String>();
		tags.add("one tag");
		tags.add("another tag");
		
		long now = System.currentTimeMillis();
		Map<String, String> headers = new HashMap<String, String>();
		headers.put(LogstashSerializer.TIMESTAMP, Long.toString(now));
		headers.put(LogstashSerializer.HOST, testHost);
		headers.put(LogstashSerializer.SRC_PATH, sourcePath);
		headers.put(LogstashSerializer.TYPE, type);
		headers.put(LogstashSerializer.SOURCE, source);
		headers.put(LogstashSerializer.TAGS, StringUtils.join(tags, LogstashSerializer.DEFAULT_TAGS_SEPARATOR));

		Event event = new SimpleEvent();
		event.setBody(body);
		event.setHeaders(headers);

		LogstashEvent logstashEvent = serializer.convertToLogstashEvent(event);

		Assert.assertNotNull(logstashEvent.getMessage());
		Assert.assertEquals(new String(body), logstashEvent.getMessage());
		
		Assert.assertNotNull(logstashEvent.getTimestamp());
		Assert.assertEquals(new Date(now), logstashEvent.getTimestamp());
		
		Assert.assertNotNull(logstashEvent.getSourceHost());
		Assert.assertEquals(testHost, logstashEvent.getSourceHost());
		
		Assert.assertNotNull(logstashEvent.getSourcePath());
		Assert.assertEquals(sourcePath, logstashEvent.getSourcePath());
		
		Assert.assertNotNull(logstashEvent.getSource());
		Assert.assertEquals(source, logstashEvent.getSource());
		
		Assert.assertNotNull(logstashEvent.getType());
		Assert.assertEquals(type, logstashEvent.getType());
		
		Assert.assertNotNull(logstashEvent.getTags());
		Assert.assertEquals(tags.size(), logstashEvent.getTags().size());
		Assert.assertTrue(CollectionUtils.isEqualCollection(tags, logstashEvent.getTags()));
		
		Assert.assertNotNull(logstashEvent.getFields());
		Assert.assertEquals(headers.size(), logstashEvent.getFields().size());
		for(String key: headers.keySet()) {
			Assert.assertTrue(logstashEvent.getFields().containsKey(key));
			Assert.assertEquals(headers.get(key), logstashEvent.getFields().get(key));
		}
	}

	
	// Verifies that Logstash style headers and body are parsed correctly
	@Test
	public void parsingLogstashEventTest() throws RedisSerializerException {

		String separator = "&";
		
		LogstashSerializer serializer = new LogstashSerializer();
		Context context = new Context();
		context.put(LogstashSerializer.CONFIGURATION_TAGS_SEPARATOR, separator);
		serializer.configure(context);
		
		byte[] body = new byte[] { '1', '2', '3', '4', '5' };
		String testHost = "testhost";
		String sourcePath = "/my/source/path";
		String type = "mytype";
		String source = "my source";
		List<String> tags = new ArrayList<String>();
		tags.add("one tag");
		tags.add("another tag");

		long now = System.currentTimeMillis();
		Map<String, String> headers = new HashMap<String, String>();
		headers.put(LogstashConstant.ATTRIBUTE_AT_TIMESTAMP, Long.toString(now));
		headers.put(LogstashConstant.ATTRIBUTE_SOURCE_HOST, testHost);
		headers.put(LogstashConstant.ATTRIBUTE_SOURCE_PATH, sourcePath);
		headers.put(LogstashConstant.ATTRIBUTE_TYPE, type);
		headers.put(LogstashConstant.ATTRIBUTE_SOURCE, source);
		headers.put(LogstashSerializer.TAGS, StringUtils.join(tags, separator));

		Event event = new SimpleEvent();
		event.setBody(body);
		event.setHeaders(headers);

		LogstashEvent logstashEvent = serializer.convertToLogstashEvent(event);

		Assert.assertNotNull(logstashEvent.getMessage());
		Assert.assertEquals(new String(body), logstashEvent.getMessage());
		
		Assert.assertNotNull(logstashEvent.getTimestamp());
		Assert.assertEquals(new Date(now), logstashEvent.getTimestamp());
		
		Assert.assertNotNull(logstashEvent.getSourceHost());
		Assert.assertEquals(testHost, logstashEvent.getSourceHost());
		
		Assert.assertNotNull(logstashEvent.getSourcePath());
		Assert.assertEquals(sourcePath, logstashEvent.getSourcePath());
		
		Assert.assertNotNull(logstashEvent.getSource());
		Assert.assertEquals(source, logstashEvent.getSource());

		Assert.assertNotNull(logstashEvent.getType());
		Assert.assertEquals(type, logstashEvent.getType());

		Assert.assertNotNull(logstashEvent.getTags());
		Assert.assertEquals(tags.size(), logstashEvent.getTags().size());
		Assert.assertTrue(CollectionUtils.isEqualCollection(tags, logstashEvent.getTags()));

		Assert.assertNotNull(logstashEvent.getFields());
		Assert.assertEquals(headers.size(), logstashEvent.getFields().size());
		for(String key: headers.keySet()) {
			Assert.assertTrue(logstashEvent.getFields().containsKey(key));
			Assert.assertEquals(headers.get(key), logstashEvent.getFields().get(key));
		}
	}
	

	// Verifies that Logstash style headers and body are parsed correctly
	@Test
	public void parsingNoHeaderTest() throws RedisSerializerException {

		LogstashSerializer serializer = new LogstashSerializer();

		byte[] body = new byte[] { '1', '2', '3', '4', '5' };

		Event event = new SimpleEvent();
		event.setBody(body);

		LogstashEvent logstashEvent = serializer.convertToLogstashEvent(event);

		Assert.assertNotNull(logstashEvent.getMessage());
		Assert.assertEquals(new String(body), logstashEvent.getMessage());
		
		Assert.assertNull(logstashEvent.getTimestamp());
		Assert.assertNull(logstashEvent.getSourceHost());
		Assert.assertNull(logstashEvent.getSourcePath());
		Assert.assertNull(logstashEvent.getSource());
		Assert.assertNull(logstashEvent.getType());
		Assert.assertNull(logstashEvent.getTags());
		Assert.assertNotNull(logstashEvent.getFields());
		Assert.assertEquals(0, logstashEvent.getFields().size());
	}

	
	//TODO Verifies that headers are not overriden
	
}
