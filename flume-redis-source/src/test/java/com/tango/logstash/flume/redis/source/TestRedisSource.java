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
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource.Status;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import com.tango.logstash.flume.redis.core.redis.MockJedisPoolFactory;

public class TestRedisSource {
    private static final Logger logger = LoggerFactory.getLogger(TestRedisSource.class);

    private class FlumeEventsMatcher extends ArgumentMatcher<List<Event>> {

        private final List<Event> expectedEvents;

        public FlumeEventsMatcher(List<Event> _expectedEvents) {
            expectedEvents = _expectedEvents;
        }

        @Override
        public boolean matches(Object argument) {
            boolean isMatch = true;

            if (argument != null && argument instanceof List<?>) {
                if (expectedEvents == null) {
                    isMatch = false;
                } else {
                    @SuppressWarnings("unchecked") List<Event> events = (List<Event>) argument;

                    if (expectedEvents.size() != events.size()) {
                        isMatch = false;
                    } else {

                        for (int i = 0; isMatch == true && i < expectedEvents.size(); i++) {
                            Event event0 = expectedEvents.get(i);
                            Event event1 = events.get(i);

                            if (event0.getBody() != event1.getBody()) {
                                isMatch = false;
                            } else {

                                isMatch = CollectionUtils.isEqualCollection(event0.getHeaders().entrySet(), event1
                                        .getHeaders().entrySet());
                            }
                        }
                    }
                }
            } else {
                isMatch = false;
            }

            return isMatch;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void noJedisPoolFactoryTest() {
        @SuppressWarnings("unused") RedisSource redisSource = new RedisSource(null);
    }

    @Test(expected = IllegalStateException.class)
    public void noConfigurationTest() {
        Jedis jedis = mock(Jedis.class);
        JedisPool jedisPool = mock(JedisPool.class);
        MockJedisPoolFactory mockJedisPoolFactory = new MockJedisPoolFactory(jedisPool, jedis);

        RedisSource redisSource = new RedisSource(mockJedisPoolFactory);
        redisSource.configure(new Context());
    }

    @Test(expected = IllegalStateException.class)
    public void noHostConfiguredTest() throws EventDeliveryException {

        Jedis jedis = mock(Jedis.class);
        JedisPool jedisPool = mock(JedisPool.class);
        MockJedisPoolFactory mockJedisPoolFactory = new MockJedisPoolFactory(jedisPool, jedis);

        RedisSource redisSource = new RedisSource(mockJedisPoolFactory);
        Context context = new Context();
        redisSource.configure(context);
    }

    @Test(expected = IllegalStateException.class)
    public void negativeBatchConfiguredTest() throws EventDeliveryException {

        Jedis jedis = mock(Jedis.class);
        JedisPool jedisPool = mock(JedisPool.class);
        MockJedisPoolFactory mockJedisPoolFactory = new MockJedisPoolFactory(jedisPool, jedis);

        RedisSource redisSource = new RedisSource(mockJedisPoolFactory);
        Context context = new Context();
        context.put(RedisSourceConfigurationConstant.HOST, "localhost");
        context.put(RedisSourceConfigurationConstant.BATCH_SIZE, "-10");
        redisSource.configure(context);
    }

    /**
     * jedis client returns an empty list. Source should back off
     * 
     * @throws EventDeliveryException
     */
    @Test
    public void noEventProcessTest() throws EventDeliveryException {
        int batchSize = 1;

        Jedis jedis = mock(Jedis.class);
        JedisPool jedisPool = mock(JedisPool.class);
        when(jedis.lrange(any(byte[].class), eq(0), eq(batchSize))).thenReturn(new ArrayList<byte[]>());

        MockJedisPoolFactory mockJedisPoolFactory = new MockJedisPoolFactory(jedisPool, jedis);

        RedisSource redisSource = new RedisSource(mockJedisPoolFactory);
        Context context = new Context();
        context.put(RedisSourceConfigurationConstant.HOST, "localhost");
        context.put(RedisSourceConfigurationConstant.BATCH_SIZE, "1");
        redisSource.configure(context);

        redisSource.doStart();
        Status status = redisSource.doProcess();
        redisSource.doStop();

        // No event returned, so source should back off
        Assert.assertEquals(Status.BACKOFF, status);
        verify(jedisPool, times(1)).getResource();
        verify(jedisPool, times(1)).returnResource(jedis);
    }

    /**
     * jedis client returns a list with a single event. Source should return status of READY
     * 
     * @throws EventDeliveryException
     */
    @SuppressWarnings("unchecked")
    @Test
    public void plainEventProcessTest() throws EventDeliveryException {
        int batchSize = 1;
        byte[] redisKey = "redisKey".getBytes();
        byte[] rawEvent = "foobar".getBytes();

        List<byte[]> redisEvents = new ArrayList<byte[]>();
        redisEvents.add(rawEvent);

        Jedis jedis = mock(Jedis.class);
        JedisPool jedisPool = mock(JedisPool.class);
        Transaction mockedJedisTransaction = mock(Transaction.class);
        when(jedis.multi()).thenReturn(mockedJedisTransaction);

        Response<List<byte[]>> mockedResponse = mock(Response.class);
        when(mockedResponse.get()).thenReturn(redisEvents);
        when(mockedJedisTransaction.lrange(any(byte[].class), anyLong(), anyLong())).thenReturn(mockedResponse);

        MockJedisPoolFactory mockJedisPoolFactory = new MockJedisPoolFactory(jedisPool, jedis);

        ChannelProcessor channelProcessor = mock(ChannelProcessor.class);

        RedisSource redisSource = new RedisSource(mockJedisPoolFactory);
        redisSource.setChannelProcessor(channelProcessor);
        Context context = new Context();
        context.put(RedisSourceConfigurationConstant.HOST, "localhost");
        context.put(RedisSourceConfigurationConstant.BATCH_SIZE, "1");
        context.put(RedisSourceConfigurationConstant.KEY, new String(redisKey));
        redisSource.configure(context);

        redisSource.doStart();
        Status status = redisSource.doProcess();
        redisSource.doStop();

        Assert.assertEquals(Status.READY, status);

        ArgumentCaptor<byte[]> argumentKey = ArgumentCaptor.forClass(byte[].class);
        ArgumentCaptor<Long> argumentStart = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> argumentEnd = ArgumentCaptor.forClass(Long.class);
        verify(mockedJedisTransaction).lrange(argumentKey.capture(), argumentStart.capture(), argumentEnd.capture());
        Assert.assertEquals(new String(redisKey), new String(argumentKey.getValue()));
        Assert.assertEquals(0L, argumentStart.getValue().longValue());
        Assert.assertEquals((long) batchSize, argumentEnd.getValue().longValue());

        List<Event> expectedEvents = new ArrayList<Event>();
        expectedEvents.add(EventBuilder.withBody(rawEvent));
        verify(channelProcessor, times(1)).processEventBatch(anyList());
        verify(channelProcessor).processEventBatch(argThat(new FlumeEventsMatcher(expectedEvents)));
        verify(jedisPool, times(1)).getResource();
        verify(jedisPool, times(1)).returnResource(jedis);
    }

    @Test
    public void jedisNullProcessTest() throws EventDeliveryException {

        JedisPool jedisPool = mock(JedisPool.class);

        MockJedisPoolFactory mockJedisPoolFactory = new MockJedisPoolFactory(jedisPool, null);

        RedisSource redisSource = new RedisSource(mockJedisPoolFactory);
        Context context = new Context();
        context.put(RedisSourceConfigurationConstant.HOST, "localhost");
        context.put(RedisSourceConfigurationConstant.BATCH_SIZE, "1");
        redisSource.configure(context);

        redisSource.doStart();
        redisSource.doProcess();
        redisSource.doStop();

        verify(jedisPool, times(1)).getResource();
        verify(jedisPool, times(0)).returnBrokenResource(any(Jedis.class));
    }

    /**
     * Source should do failed process if there is any failed event
     * 
     * @throws EventDeliveryException
     */
    @SuppressWarnings("unchecked")
    @Test
    public void failedProcessTest() throws EventDeliveryException {
        byte[] rawEvent = "foobar".getBytes();
        byte[] rawEvent2 = "tututititoto".getBytes();

        List<byte[]> redisEvents = new ArrayList<byte[]>();
        redisEvents.add(rawEvent);
        List<byte[]> redisEvents2 = new ArrayList<byte[]>();
        redisEvents2.add(rawEvent2);

        /**
         * First we make a transaction failed on a ChannelException in order to have the source save the event
         */
        Jedis jedis = mock(Jedis.class);
        JedisPool jedisPool = mock(JedisPool.class);
        Transaction mockedJedisTransaction = mock(Transaction.class);
        when(jedis.multi()).thenReturn(mockedJedisTransaction);

        Response<List<byte[]>> mockedResponse = mock(Response.class);
        when(mockedResponse.get()).thenReturn(redisEvents);
        when(mockedJedisTransaction.lrange(any(byte[].class), anyLong(), anyLong())).thenReturn(mockedResponse);

        MockJedisPoolFactory mockJedisPoolFactory = new MockJedisPoolFactory(jedisPool, jedis);

        ChannelProcessor channelProcessor = mock(ChannelProcessor.class);
        doThrow(ChannelException.class).when(channelProcessor).processEventBatch(anyList());

        RedisSource redisSource = new RedisSource(mockJedisPoolFactory);
        redisSource.setChannelProcessor(channelProcessor);
        Context context = new Context();
        context.put(RedisSourceConfigurationConstant.HOST, "localhost");
        context.put(RedisSourceConfigurationConstant.BATCH_SIZE, "1");
        redisSource.configure(context);

        redisSource.doStart();
        Status status = redisSource.doProcess();
        redisSource.doStop();

        Assert.assertEquals(Status.BACKOFF, status);

        /**
         * Point the jedis source to a different set of event and verified the failed ones from before are being
         * processed
         */
        when(mockedResponse.get()).thenReturn(redisEvents2);
        reset(channelProcessor);
        reset(jedisPool);

        redisSource.doStart();
        Status status2 = redisSource.doProcess();
        redisSource.doStop();
        
        Assert.assertEquals(Status.READY, status2);
        List<Event> expectedEvents = new ArrayList<Event>();
        expectedEvents.add(EventBuilder.withBody(rawEvent));
        verify(channelProcessor, times(1)).processEventBatch(anyList());
        verify(channelProcessor).processEventBatch(argThat(new FlumeEventsMatcher(expectedEvents)));
        verify(jedisPool, times(0)).getResource();
        verify(jedisPool, times(0)).returnBrokenResource(any(Jedis.class));
    }

}
