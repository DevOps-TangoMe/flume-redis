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
package com.tango.logstash.flume.redis.sink;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.Sink;
import org.apache.flume.SinkFactory;
import org.apache.flume.sink.DefaultSinkFactory;
import org.junit.Assert;
import org.junit.Test;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import com.tango.logstash.flume.redis.core.redis.JedisPoolFactory;

public class TestRedisSinkCreation {

    private void verifySinkCreation(String name, String type, Class<?> typeClass) throws FlumeException {
        SinkFactory sinkFactory = new DefaultSinkFactory();
        Sink sink = sinkFactory.create(name, type);
        Assert.assertNotNull(sink);
        Assert.assertTrue(typeClass.isInstance(sink));
    }

    @Test
    public void testSinkCreation() {
        verifySinkCreation("redis-sink", RedisSink.class.getName(), RedisSink.class);
    }

    /**
     * Verify that setting a timeout in the configuration does not impact the database setting
     */
    @Test
    public void timeoutConfiguredTest() {

        String host = "localhost";
        int timeout = 10;

        JedisPoolFactory mockJedisPoolFactory = mock(JedisPoolFactory.class);

        RedisSink redisSink = new RedisSink(mockJedisPoolFactory);

        Channel channel = mock(Channel.class);
        redisSink.setChannel(channel);

        Context context = new Context();
        context.put(RedisSinkConfigurationConstant.HOST, host);
        context.put(RedisSinkConfigurationConstant.BATCH_SIZE, "10");
        context.put(RedisSinkConfigurationConstant.TIMEOUT, Integer.toString(timeout));
        redisSink.configure(context);

        redisSink.start();

        verify(mockJedisPoolFactory).create(any(JedisPoolConfig.class), eq(host), eq(Protocol.DEFAULT_PORT),
                eq(timeout), isNull(String.class), eq(Protocol.DEFAULT_DATABASE));
    }

}
