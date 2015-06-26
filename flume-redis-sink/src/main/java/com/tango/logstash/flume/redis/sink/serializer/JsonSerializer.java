package com.tango.logstash.flume.redis.sink.serializer;

import com.google.gson.JsonObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;

import java.util.Map;

/**
 * Created by abootman on 6/25/15.
 */
public class JsonSerializer implements Serializer {
    @Override
    public byte[] serialize(Event event) throws RedisSerializerException {
        JsonObject jsonObject = new JsonObject();
        for (Map.Entry<String, String> entry : event.getHeaders().entrySet()) {
            jsonObject.addProperty(entry.getKey(), entry.getValue());
        }
        return jsonObject.toString().getBytes();
    }

    @Override
    public void configure(Context context) {

    }

    @Override
    public void configure(ComponentConfiguration componentConfiguration) {

    }
}
