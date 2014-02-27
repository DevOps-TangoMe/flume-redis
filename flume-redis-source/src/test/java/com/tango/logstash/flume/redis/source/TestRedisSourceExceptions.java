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
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.tango.logstash.flume.redis.core.redis.MockJedisPoolFactory;
import com.tango.logstash.flume.redis.source.serializer.RedisSerializerException;

@RunWith(Parameterized.class)
public class TestRedisSourceExceptions {

    @Parameters
    public static Collection<Object[]> generateData() {
        return Arrays.asList(new Object[][]{{IllegalArgumentException.class}, {JedisConnectionException.class},
                {RedisSerializerException.class}});
    }

    private final Class<? extends Throwable> clazz;

    public TestRedisSourceExceptions(Class<? extends Throwable> _class) {
        this.clazz = _class;
    }

    @SuppressWarnings("unchecked")
    @Test
    public void ThrowableThrownExceptionTest() throws EventDeliveryException {
        Jedis jedis = mock(Jedis.class);
        JedisPool jedisPool = mock(JedisPool.class);
        redis.clients.jedis.Transaction mockedJedisTransaction = mock(redis.clients.jedis.Transaction.class);
        when(jedis.multi()).thenReturn(mockedJedisTransaction);
        
        when(mockedJedisTransaction.lrange(any(byte[].class), anyLong(), anyLong())).thenThrow(clazz);

        MockJedisPoolFactory mockJedisPoolFactory = new MockJedisPoolFactory(jedisPool, jedis);

        RedisSource redisSource = new RedisSource(mockJedisPoolFactory);
        Context context = new Context();
        context.put(RedisSourceConfigurationConstant.HOST, "localhost");
        context.put(RedisSourceConfigurationConstant.BATCH_SIZE, "1");
        redisSource.configure(context);

        redisSource.doStart();
        redisSource.doProcess();

        verify(jedisPool, times(1)).getResource();
        verify(jedisPool, times(1)).returnResource(jedis);
        if (JedisConnectionException.class.equals(clazz) == true) {
            verify(jedisPool, times(1)).returnBrokenResource(jedis);
        }

        redisSource.doStop();

    }

}
