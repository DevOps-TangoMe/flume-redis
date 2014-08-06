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

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.Assert;
import org.junit.Test;

import com.tango.logstash.flume.redis.sink.serializer.PlainSerializer;
import com.tango.logstash.flume.redis.sink.serializer.RedisSerializerException;
import com.tango.logstash.flume.redis.sink.serializer.Serializer;

public class TestPlainSerializer {

	@Test(expected = RedisSerializerException.class)
	public void nullEventTest() throws RedisSerializerException {

		Serializer serializer = new PlainSerializer();
		serializer.serialize(null);
	}

	@Test
	public void eventBodyTest() throws RedisSerializerException {

		Serializer serializer = new PlainSerializer();

		byte[] input = new byte[] { '1', '2', '3', '4', '5' };

		Map<String, String> headers = new HashMap<String, String>();
		headers.put("bogus", "reallybogus");

		Event event = new SimpleEvent();
		event.setHeaders(headers);
		event.setBody(input);
		byte[] output = serializer.serialize(event);

		Assert.assertEquals(input.length, output.length);
		for (int i = 0; i < input.length; i++) {
			Assert.assertEquals(input[i], output[i]);
		}
	}

}
