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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;

import com.tango.logstash.flume.redis.core.LogstashConstant;
import com.tango.logstash.flume.redis.core.LogstashEvent;

public class LogstashSerializer implements Serializer {

	public static final String CONFIGURATION_TAGS_SEPARATOR = "tags_separator";
	
	public static final String HOST = "host";
	public static final String SOURCE = "source";
	public static final String SRC_PATH = "src_path";
	public static final String TIMESTAMP = "timestamp";
	public static final String TYPE = "type";
	public static final String TAGS = "tags";

	public static final String DEFAULT_TAGS_SEPARATOR = ",";
	
	private ObjectMapper objectMapper = new ObjectMapper();

	private String tagsSeparator = DEFAULT_TAGS_SEPARATOR; 
	
	public void configure(Context context) {
		objectMapper.configure(Feature.WRITE_DATES_AS_TIMESTAMPS, false);
		
		tagsSeparator = context.getString(CONFIGURATION_TAGS_SEPARATOR, DEFAULT_TAGS_SEPARATOR);
	}

	public void configure(ComponentConfiguration conf) {
	}

	public LogstashEvent convertToLogstashEvent(Event event) throws RedisSerializerException {
		
		if (event == null) {
			throw new RedisSerializerException("Event cannot be null");
		}
		
		LogstashEvent logstashEvent = new LogstashEvent();
		final Map<String, String> headers = event.getHeaders();

		/**
		 * Set message body
		 */
		logstashEvent.setMessage(new String(event.getBody()));

		/**
		 * Set fields
		 */
		for (Entry<String, String> entry : headers.entrySet()) {
			logstashEvent.putField(entry.getKey(), entry.getValue());
		}

		/**
		 * Set timestamp
		 */
		String timestamp = null;
		if (StringUtils.isNotBlank(headers.get(LogstashConstant.ATTRIBUTE_AT_TIMESTAMP))) {
			timestamp = headers.get(LogstashConstant.ATTRIBUTE_AT_TIMESTAMP);
		} else if (StringUtils.isNotBlank(headers.get(TIMESTAMP))) {
			timestamp = headers.get(TIMESTAMP);
		}

		if (timestamp != null) {
			long timestampMs = Long.parseLong(timestamp);
			logstashEvent.setTimestamp(new Date(timestampMs));
		}

		/**
		 * Set source
		 */
		String source = null;
		if (StringUtils.isNotBlank(headers.get(LogstashConstant.ATTRIBUTE_SOURCE))) {
			source = headers.get(LogstashConstant.ATTRIBUTE_SOURCE);
		} else if (StringUtils.isNotBlank(headers.get(SOURCE))) {
			source = headers.get(SOURCE);
		}

		if (source != null) {
			logstashEvent.setSource(source);
		}

		/**
		 * Set host
		 */
		String host = null;
		if (StringUtils.isNotBlank(headers.get(LogstashConstant.ATTRIBUTE_SOURCE_HOST))) {
			host = headers.get(LogstashConstant.ATTRIBUTE_SOURCE_HOST);
		} else if (StringUtils.isNotBlank(headers.get(HOST))) {
			host = headers.get(HOST);
		}

		if (host != null) {
			logstashEvent.setSourceHost(host);
		}

		/**
		 * Set src_path
		 */
		String sourcePath = null;
		if (StringUtils.isNotBlank(headers.get(LogstashConstant.ATTRIBUTE_SOURCE_PATH))) {
			sourcePath = headers.get(LogstashConstant.ATTRIBUTE_SOURCE_PATH);
		} else if (StringUtils.isNotBlank(headers.get(SRC_PATH))) {
			sourcePath = headers.get(SRC_PATH);
		}

		if (sourcePath != null) {
			logstashEvent.setSourcePath(sourcePath);
		}

		/**
		 * Set type
		 */
		String type = null;
		if (StringUtils.isNotBlank(headers.get(LogstashConstant.ATTRIBUTE_TYPE))) {
			type = headers.get(LogstashConstant.ATTRIBUTE_TYPE);
		} else if (StringUtils.isNotBlank(headers.get(TYPE))) {
			type = headers.get(TYPE);
		}

		if (type != null) {
			logstashEvent.setType(type);
		}

		/**
		 * Set tags
		 */
		String tags = null;
		if (StringUtils.isNotBlank(headers.get(LogstashConstant.ATTRIBUTE_TAGS))) {
			tags = headers.get(LogstashConstant.ATTRIBUTE_TAGS);
		} else if (StringUtils.isNotBlank(headers.get(TAGS))) {
			tags = headers.get(TAGS);
		}

		if (StringUtils.isNotBlank(tags)) {
			String[] allTags = tags.split(tagsSeparator);
			List<String> tagsList = new ArrayList<String>();
			for(String tag: allTags) {
				if(StringUtils.isNotBlank(tag)) {
					tagsList.add(tag);
				}
			}
			logstashEvent.setTags(tagsList);
		}

		
		return logstashEvent;
	}

	public byte[] serialize(Event event) throws RedisSerializerException {
		byte[] result = null;

		if (event == null) {
			throw new RedisSerializerException("Event cannot be null");
		}

		LogstashEvent logstashEvent = convertToLogstashEvent(event);

		/**
		 * Serialize the event
		 */
		try {
			result = objectMapper.writeValueAsBytes(logstashEvent);
		} catch (IOException e) {
			throw new RedisSerializerException("Could not serialize event", e);
		}

		return result;

	}
}
