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
package com.tango.logstash.flume.redis.source.serializer;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.event.SimpleEvent;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.tango.logstash.flume.redis.core.LogstashEvent;

public class LogstashDeSerializer implements Serializer {

    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    public static final String FIELD_AT_TIMESTAMP = "@timestamp";
    public static final String FIELD_AT_TYPE = "@type";
    public static final String FIELD_AT_SOURCE_PATH = "@source_path";
    public static final String FIELD_AT_SOURCE_HOST = "@source_host";
    public static final String FIELD_AT_SOURCE = "@source";
    public static final String FIELD_AT_VERSION = "@version";
    public static final String FIELD_TAGS = "tags";

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Context context) {
        // TODO Auto-generated method stub

    }

    @Override
    public void configure(ComponentConfiguration conf) {
        // TODO Auto-generated method stub

    }

    @Override
    public Event parseEvent(byte[] eventSource) throws RedisSerializerException {
        Event flumeEvent = new SimpleEvent();

        if (eventSource == null) {
            throw new IllegalArgumentException("Event source cannot be null");
        }

        try {
            LogstashEvent logstashEvent = objectMapper.readValue(eventSource, LogstashEvent.class);

            String body = "";
            if (StringUtils.isNotBlank(logstashEvent.getMessage())) {
                body = logstashEvent.getMessage();
            }

            Map<String, String> headers = new HashMap<String, String>();

            if (logstashEvent.getFields() != null) {
                headers.putAll(logstashEvent.getFields());
            }

            if (StringUtils.isNotBlank(logstashEvent.getSource())) {
                headers.put(FIELD_AT_SOURCE, logstashEvent.getSource());
            }

            if (StringUtils.isNotBlank(logstashEvent.getSourceHost())) {
                headers.put(FIELD_AT_SOURCE_HOST, logstashEvent.getSourceHost());
            }

            if (StringUtils.isNotBlank(logstashEvent.getSourcePath())) {
                headers.put(FIELD_AT_SOURCE_PATH, logstashEvent.getSourcePath());
            }

            if (StringUtils.isNotBlank(logstashEvent.getType())) {
                headers.put(FIELD_AT_TYPE, logstashEvent.getType());
            }

            if (logstashEvent.getVersion() != null) {
                headers.put(FIELD_AT_VERSION, logstashEvent.getVersion().toString());
            }
            
            if (logstashEvent.getTimestamp() != null) {
                DateFormat dateFormat = new SimpleDateFormat(TIMESTAMP_FORMAT);
                Date timestamp = logstashEvent.getTimestamp();
                headers.put(FIELD_AT_TIMESTAMP, dateFormat.format(timestamp));
            }

            if(CollectionUtils.isNotEmpty(logstashEvent.getTags())) {
                headers.put(FIELD_TAGS, StringUtils.join(logstashEvent.getTags(), ","));
            }
            
            flumeEvent = EventBuilder.withBody(body.getBytes(), headers);

        } catch (JsonParseException e) {
            throw new RedisSerializerException("Could not parse event: [" + new String(eventSource) + "]", e);
        } catch (JsonMappingException e) {
            throw new RedisSerializerException("Could not parse event: [" + new String(eventSource) + "]", e);
        } catch (IOException e) {
            throw new RedisSerializerException("Could not parse event: [" + new String(eventSource) + "]", e);
        }

        return flumeEvent;
    }
}
