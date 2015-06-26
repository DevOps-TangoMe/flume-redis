package com.tango.logstash.flume.redis.sink.serializer;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by abootman on 6/26/15.
 */
public class TestJsonSerializer {

    private String serialize(Map<String, String> eventHeaders) throws RedisSerializerException {
        Serializer serializer = new JsonSerializer();
        Event event = mock(Event.class);
        when(event.getHeaders()).thenReturn(eventHeaders);
        return new String(serializer.serialize(event));
    }

    @Test
    public void testSerializerNoEventHeaders() throws RedisSerializerException {
        String res = serialize(new HashMap<String, String>());
        assertTrue(res.equals("{}"));
    }

    @Test
    public void testSerializer() throws RedisSerializerException {
        String res = serialize(new HashMap<String, String>() {{
                                   put("message", "message \"abc\" def");
                                   put("host", "this-host");
                                   put("@timestamp", "06/19/2015 10:02:33.12345");
                               }}
        );

        String[] parts = res.split("(\\{\"|\"\\:\"|\",\"|\"\\})");
        int count = 0;
        int keyCount = 0;
        int valueCount = 0;
        for (String part : parts) {
            ++count;
            if (part.isEmpty() || part.equals("\n")) {
                // skip
            } else if ("message".equals(part)) {
                ++keyCount;
            } else if ("host".equals(part)) {
                ++keyCount;
            } else if ("@timestamp".equals(part)) {
                ++keyCount;
            } else if ("message \\\"abc\\\" def".equals(part)) {
                ++valueCount;
            } else if ("this-host".equals(part)) {
                ++valueCount;
            } else if ("06/19/2015 10:02:33.12345".equals(part)) {
                ++valueCount;
            } else {
                throw new RuntimeException(String.format("unknown element <%s>", part));
            }
        }
        assertTrue(count == 7);
        assertTrue(keyCount == 3);
        assertTrue(valueCount == 3);
    }

}
