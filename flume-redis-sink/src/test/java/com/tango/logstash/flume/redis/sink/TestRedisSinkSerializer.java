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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.junit.Test;

import com.tango.logstash.flume.redis.core.redis.MockJedisPoolFactory;
import com.tango.logstash.flume.redis.sink.RedisSink;
import com.tango.logstash.flume.redis.sink.RedisSinkConfigurationConstant;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class TestRedisSinkSerializer {
	
	@Test(expected=RuntimeException.class)
	public void simpleProcessTest() throws EventDeliveryException {
		
		Jedis jedis = mock(Jedis.class);
		JedisPool jedisPool = mock(JedisPool.class);
		MockJedisPoolFactory mockJedisPoolFactory = new MockJedisPoolFactory(jedisPool, jedis);

		Channel channel = mock(Channel.class);
		Transaction transactionMock = mock(Transaction.class);
		when(channel.getTransaction()).thenReturn(transactionMock);

		RedisSink redisSink = new RedisSink(mockJedisPoolFactory);
		redisSink.setChannel(channel);

		Context context = new Context();
		context.put(RedisSinkConfigurationConstant.HOST, "localhost");
		context.put(RedisSinkConfigurationConstant.SERIALIZER, "you.must.be kidding.me.if.this.c/lass.exit?");
		redisSink.configure(context);
	}

}
