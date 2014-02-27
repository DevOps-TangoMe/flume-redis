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

import java.util.Map;

import org.apache.flume.Event;
import org.junit.Assert;
import org.junit.Test;

public class TestLogstashDeSerializer {

    @Test(expected = IllegalArgumentException.class)
    public void nullEvent() throws RedisSerializerException {
        LogstashDeSerializer logstashDeSerializer = new LogstashDeSerializer();
        logstashDeSerializer.parseEvent(null);
    }

    @Test
    public void testNewLogstashMessageFormat() throws RedisSerializerException {
        final String EXPECTED_VERSION = "1";
        final String EXPECTED_TYPE = "authConsumer";

        final String TEST_INPUT_EVENT = new String("{\"message\":\"111.111.111.11 - - "
                + "[06/Feb/2014:02:36:56 +0000] "
                + "\\\"POST /facilitator/rest/validation/v1/tango_validation/4564514f4gh4tr48ds "
                + "HTTP/1.1\\\" 200 304 B \\\"proxy-handler-worker-4\\\" 8 ms us0101bac020.tangome.gbl\","
                + "\"@timestamp\":\"2012-12-12T06:45:06.850Z\"," + "\"@version\":\"" + EXPECTED_VERSION + "\","
                + "\"type\":\"" + EXPECTED_TYPE + "\"," + "\"tags\":[\"authConsumer\"],"
                + "\"host\":\"us0101bac020.tangome.gbl\"," + "\"path\":\"/local/authConsumer/logs/access.log\"}");

        LogstashDeSerializer logstashDeSerializer = new LogstashDeSerializer();
        Event parsedEvent = logstashDeSerializer.parseEvent(TEST_INPUT_EVENT.getBytes());

        Assert.assertNotNull(parsedEvent);
        Map<String, String> headers = parsedEvent.getHeaders();
        Assert.assertNotNull(headers);
        Assert.assertTrue(headers.containsKey(LogstashDeSerializer.FIELD_AT_TIMESTAMP));
        Assert.assertTrue(headers.containsKey(LogstashDeSerializer.FIELD_AT_SOURCE_HOST));
        Assert.assertTrue(headers.containsKey(LogstashDeSerializer.FIELD_AT_SOURCE_PATH));
        Assert.assertTrue(headers.containsKey(LogstashDeSerializer.FIELD_AT_VERSION));
        Assert.assertTrue(headers.containsKey(LogstashDeSerializer.FIELD_TAGS));
        
        Assert.assertEquals(EXPECTED_VERSION, headers.get(LogstashDeSerializer.FIELD_AT_VERSION));
        Assert.assertEquals(EXPECTED_TYPE, headers.get(LogstashDeSerializer.FIELD_AT_TYPE));
    }
}
