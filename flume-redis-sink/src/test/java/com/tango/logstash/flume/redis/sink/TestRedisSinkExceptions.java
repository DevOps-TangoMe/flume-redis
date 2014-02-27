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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.event.SimpleEvent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.tango.logstash.flume.redis.core.redis.MockJedisPoolFactory;
import com.tango.logstash.flume.redis.sink.RedisSink;
import com.tango.logstash.flume.redis.sink.RedisSinkConfigurationConstant;
import com.tango.logstash.flume.redis.sink.serializer.RedisSerializerException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

@RunWith(Parameterized.class)
public class TestRedisSinkExceptions {

	@Parameters
	public static Collection<Object[]> generateData() {
		return Arrays.asList(new Object[][] { { IllegalArgumentException.class }, { JedisConnectionException.class },
				{ RedisSerializerException.class } });
	}

	private final Class<? extends Throwable> clazz;

	public TestRedisSinkExceptions(Class<? extends Throwable> _class) {
		this.clazz = _class;
	}

	@SuppressWarnings("unchecked")
    @Test
	public void ThrowableThrownExceptionTest() throws EventDeliveryException {
		Jedis jedis = mock(Jedis.class);
		// Not really true, but fits the requirement
		when(jedis.lpush(any(byte[].class), any(byte[].class))).thenThrow(clazz);

		JedisPool jedisPool = mock(JedisPool.class);
		MockJedisPoolFactory mockJedisPoolFactory = new MockJedisPoolFactory(jedisPool, jedis);

		Channel channel = mock(Channel.class);
		Transaction transactionMock = mock(Transaction.class);
		when(channel.getTransaction()).thenReturn(transactionMock);

		Event testEvent = new SimpleEvent();
		byte[] testBody = new byte[] { 'b', 'o', 'd', 'y' };
		testEvent.setBody(testBody);
		when(channel.take()).thenReturn(testEvent);

		RedisSink redisSink = new RedisSink(mockJedisPoolFactory);
		redisSink.setChannel(channel);

		Context context = new Context();
		context.put(RedisSinkConfigurationConstant.HOST, "localhost");
		redisSink.configure(context);

		redisSink.start();

		redisSink.process();

		verify(channel, times(1)).getTransaction();
		verify(channel, times(1)).take();

		verify(transactionMock, times(1)).begin();
		verify(transactionMock, times(1)).close();
		verify(transactionMock, times(0)).commit();
		verify(transactionMock, times(1)).rollback();		

		verify(jedisPool, times(1)).getResource();
		verify(jedisPool, times(1)).returnResource(jedis);

		verify(jedis, times(1)).lpush(eq(RedisSinkConfigurationConstant.DEFAULT_KEY.getBytes()), any(byte[][].class));
	}

}
