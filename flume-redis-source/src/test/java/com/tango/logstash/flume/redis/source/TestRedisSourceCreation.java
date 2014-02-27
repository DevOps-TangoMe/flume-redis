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
package com.tango.logstash.flume.redis.source;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Source;
import org.apache.flume.SourceFactory;
import org.apache.flume.source.DefaultSourceFactory;
import org.junit.Assert;
import org.junit.Test;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import com.tango.logstash.flume.redis.core.redis.JedisPoolFactory;

public class TestRedisSourceCreation {

    private void verifySourceCreation(String name, String type, Class<?> typeClass) throws FlumeException {
        SourceFactory sourceFactory = new DefaultSourceFactory();
        Source source = sourceFactory.create(name, type);
        Assert.assertNotNull(source);
        Assert.assertTrue(typeClass.isInstance(source));
    }

    @Test
    public void testSourceCreation() {
        verifySourceCreation("redis-source", RedisSource.class.getName(), RedisSource.class);
    }

    /**
     * Verify that setting a timeout in the configuration does not impact the database setting
     * 
     * @throws EventDeliveryException
     */
    @Test
    public void timeoutConfiguredTest() throws EventDeliveryException {

        String host = "localhost";
        int timeout = 10;

        JedisPoolFactory mockJedisPoolFactory = mock(JedisPoolFactory.class);

        RedisSource redisSource = new RedisSource(mockJedisPoolFactory);
        Context context = new Context();
        context.put(RedisSourceConfigurationConstant.HOST, host);
        context.put(RedisSourceConfigurationConstant.BATCH_SIZE, "10");
        context.put(RedisSourceConfigurationConstant.TIMEOUT, Integer.toString(timeout));
        redisSource.configure(context);
        redisSource.doStart();

        verify(mockJedisPoolFactory).create(any(JedisPoolConfig.class), eq(host), eq(Protocol.DEFAULT_PORT),
                eq(timeout), isNull(String.class), eq(Protocol.DEFAULT_DATABASE));
    }

}
