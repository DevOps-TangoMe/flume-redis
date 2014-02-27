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
import junit.framework.Assert;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink.Status;
import org.apache.flume.Transaction;
import org.apache.flume.event.SimpleEvent;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import com.tango.logstash.flume.redis.core.redis.MockJedisPoolFactory;

public class TestRedisSink {

    @Test(expected = IllegalArgumentException.class)
    public void noJedisPoolFactoryTest() {
        @SuppressWarnings("unused") RedisSink redisSink = new RedisSink(null);
    }

    @Test(expected = IllegalStateException.class)
    public void noConfigurationTest() {
        Jedis jedis = mock(Jedis.class);
        JedisPool jedisPool = mock(JedisPool.class);
        MockJedisPoolFactory mockJedisPoolFactory = new MockJedisPoolFactory(jedisPool, jedis);

        RedisSink redisSink = new RedisSink(mockJedisPoolFactory);
        redisSink.configure(new Context());
    }

    @Test(expected = IllegalStateException.class)
    public void noHostConfiguredTest() throws EventDeliveryException {

        Jedis jedis = mock(Jedis.class);
        JedisPool jedisPool = mock(JedisPool.class);
        MockJedisPoolFactory mockJedisPoolFactory = new MockJedisPoolFactory(jedisPool, jedis);

        Channel channel = mock(Channel.class);
        Transaction transactionMock = mock(Transaction.class);
        when(channel.getTransaction()).thenReturn(transactionMock);

        RedisSink redisSink = new RedisSink(mockJedisPoolFactory);
        redisSink.setChannel(channel);

        Context context = new Context();
        redisSink.configure(context);
    }

    @Test(expected = IllegalStateException.class)
    public void negativeBatchConfiguredTest() throws EventDeliveryException {

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
        context.put(RedisSinkConfigurationConstant.BATCH_SIZE, "-10");
        redisSink.configure(context);
    }

    @Test
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
        redisSink.configure(context);

        redisSink.start();

        redisSink.process();

        verify(channel, times(1)).getTransaction();
        verify(channel, times(1)).take();

        verify(transactionMock, times(1)).begin();
        verify(transactionMock, times(1)).close();
        verify(transactionMock, times(1)).commit();
        verify(transactionMock, times(0)).rollback();

        verify(jedisPool, times(1)).getResource();
        verify(jedisPool, times(1)).returnResource(jedis);
    }

    @Test
    public void startStopStartProcessTest() throws EventDeliveryException {
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
        redisSink.configure(context);

        for (int i = 0; i < 10; i++) {
            redisSink.start();
            redisSink.stop();
        }

        redisSink.start();
        redisSink.process();

        verify(channel, times(1)).getTransaction();
        verify(channel, times(1)).take();

        verify(transactionMock, times(1)).begin();
        verify(transactionMock, times(1)).close();
        verify(transactionMock, times(1)).commit();
        verify(transactionMock, times(0)).rollback();

        verify(jedisPool, times(1)).getResource();
        verify(jedisPool, times(1)).returnResource(jedis);
    }

    @Test
    public void stopStartStopStartProcessTest() throws EventDeliveryException {
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
        redisSink.configure(context);

        for (int i = 0; i < 10; i++) {
            redisSink.stop();
            redisSink.start();
        }

        redisSink.process();

        verify(channel, times(1)).getTransaction();
        verify(channel, times(1)).take();

        verify(transactionMock, times(1)).begin();
        verify(transactionMock, times(1)).close();
        verify(transactionMock, times(1)).commit();
        verify(transactionMock, times(0)).rollback();

        verify(jedisPool, times(1)).getResource();
        verify(jedisPool, times(1)).returnResource(jedis);
    }

    @Test(expected = EventDeliveryException.class)
    public void processWithoutStartTest() throws EventDeliveryException {
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
        redisSink.configure(context);

        // No start on purpose

        redisSink.process();

        verify(channel, times(1)).getTransaction();
        verify(channel, times(1)).take();

        verify(transactionMock, times(1)).begin();
        verify(transactionMock, times(1)).close();
        verify(transactionMock, times(1)).commit();
        verify(transactionMock, times(0)).rollback();

        verify(jedisPool, times(1)).getResource();
        verify(jedisPool, times(1)).returnResource(jedis);
    }

    @Test
    public void simpleProcessOneEventTest() throws EventDeliveryException {
        Jedis jedis = mock(Jedis.class);
        JedisPool jedisPool = mock(JedisPool.class);
        MockJedisPoolFactory mockJedisPoolFactory = new MockJedisPoolFactory(jedisPool, jedis);

        Channel channel = mock(Channel.class);
        Transaction transactionMock = mock(Transaction.class);
        when(channel.getTransaction()).thenReturn(transactionMock);

        Event testEvent = new SimpleEvent();
        byte[] testBody = new byte[]{'b', 'o', 'd', 'y'};
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
        verify(transactionMock, times(1)).commit();
        verify(transactionMock, times(0)).rollback();

        verify(jedisPool, times(1)).getResource();
        verify(jedisPool, times(1)).returnResource(jedis);

        verify(jedis, times(1)).lpush(eq(RedisSinkConfigurationConstant.DEFAULT_KEY.getBytes()), any(byte[][].class));
    }

    @Test
    public void processMultiplEventsInOneBatchTest() throws EventDeliveryException {

        int batchSize = 4;

        Jedis jedis = mock(Jedis.class);
        JedisPool jedisPool = mock(JedisPool.class);
        MockJedisPoolFactory mockJedisPoolFactory = new MockJedisPoolFactory(jedisPool, jedis);

        Channel channel = mock(Channel.class);
        Transaction transactionMock = mock(Transaction.class);
        when(channel.getTransaction()).thenReturn(transactionMock);

        Event testEvent = new SimpleEvent();
        byte[] testBody = new byte[]{'b', 'o', 'd', 'y'};
        testEvent.setBody(testBody);
        when(channel.take()).thenReturn(testEvent);

        RedisSink redisSink = new RedisSink(mockJedisPoolFactory);
        redisSink.setChannel(channel);

        Context context = new Context();
        context.put(RedisSinkConfigurationConstant.HOST, "localhost");
        context.put(RedisSinkConfigurationConstant.BATCH_SIZE, Integer.toString(batchSize));
        redisSink.configure(context);

        redisSink.start();

        redisSink.process();

        verify(channel, times(1)).getTransaction();
        verify(channel, times(batchSize)).take();

        verify(transactionMock, times(1)).begin();
        verify(transactionMock, times(1)).close();
        verify(transactionMock, times(1)).commit();
        verify(transactionMock, times(0)).rollback();

        verify(jedisPool, times(1)).getResource();
        verify(jedisPool, times(1)).returnResource(jedis);

        verify(jedis, times(1)).lpush(eq(RedisSinkConfigurationConstant.DEFAULT_KEY.getBytes()), any(byte[].class),
                any(byte[].class), any(byte[].class), any(byte[].class));
    }

    @Test
    public void processLessEventsThanBatchTest() throws EventDeliveryException {

        int batchSize = 4;

        Jedis jedis = mock(Jedis.class);
        JedisPool jedisPool = mock(JedisPool.class);
        MockJedisPoolFactory mockJedisPoolFactory = new MockJedisPoolFactory(jedisPool, jedis);

        Channel channel = mock(Channel.class);
        Transaction transactionMock = mock(Transaction.class);
        when(channel.getTransaction()).thenReturn(transactionMock);

        Event testEvent = new SimpleEvent();
        byte[] testBody = new byte[]{'b', 'o', 'd', 'y'};
        testEvent.setBody(testBody);

        /**
         * This will return 2 events and then null. So I expect only one batch with 2 events since batch size is set to
         * 4
         */
        when(channel.take()).thenReturn(testEvent).thenReturn(testEvent).thenReturn(null);

        RedisSink redisSink = new RedisSink(mockJedisPoolFactory);
        redisSink.setChannel(channel);

        Context context = new Context();
        context.put(RedisSinkConfigurationConstant.HOST, "localhost");
        context.put(RedisSinkConfigurationConstant.BATCH_SIZE, Integer.toString(batchSize));
        redisSink.configure(context);

        redisSink.start();

        Status status = redisSink.process();

        Assert.assertEquals(Status.BACKOFF, status);

        verify(channel, times(1)).getTransaction();
        verify(channel, times(3)).take(); // 3 since we only return 2 events
                                          // plus an additional one which will
                                          // return null

        verify(transactionMock, times(1)).begin();
        verify(transactionMock, times(1)).close();
        verify(transactionMock, times(1)).commit();
        verify(transactionMock, times(0)).rollback();

        verify(jedisPool, times(1)).getResource();
        verify(jedisPool, times(1)).returnResource(jedis);

        verify(jedis, times(1)).lpush(eq(RedisSinkConfigurationConstant.DEFAULT_KEY.getBytes()), any(byte[].class),
                any(byte[].class));
    }

}
