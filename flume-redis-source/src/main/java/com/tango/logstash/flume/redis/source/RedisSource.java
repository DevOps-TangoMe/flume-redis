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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractPollableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.tango.logstash.flume.redis.core.redis.JedisPoolFactory;
import com.tango.logstash.flume.redis.core.redis.JedisPoolFactoryImpl;
import com.tango.logstash.flume.redis.source.serializer.RedisSerializerException;
import com.tango.logstash.flume.redis.source.serializer.Serializer;

public class RedisSource extends AbstractPollableSource {
    private static final Logger logger = LoggerFactory.getLogger(RedisSource.class);

    private static final String WHEN_EXHAUSTED_GROW = "WHEN_EXHAUSTED_GROW";
    private static final String WHEN_EXHAUSTED_FAIL = "WHEN_EXHAUSTED_FAIL";
    private static final String WHEN_EXHAUSTED_BLOCK = "WHEN_EXHAUSTED_BLOCK";

    private static final ThreadLocal<List<Event>> previousFailedEvents = new ThreadLocal<List<Event>>() {
        @Override
        protected List<Event> initialValue() {
            return null;
        }
    };

    /**
     * Configuration attributes
     */
    private String host = null;
    private Integer port = null;
    private Integer timeout = null;
    private String password = null;
    private Integer database = null;
    private byte[] redisKey = null;
    private Integer batchSize = null;
    private Serializer serializer = null;

    private Integer maxActive = null;
    private Integer maxIdle = null;
    private Integer minIdle = null;
    private Integer numTestsPerEvictionRun = null;
    private Long maxWait = null;
    private Long minEvictableIdleTimeMillis = null;
    private Long softMinEvictableIdleTimeMillis = null;
    private Long timeBetweenEvictionRunsMillis = null;
    private Boolean testOnBorrow = null;
    private Boolean testOnReturn = null;
    private Boolean testWhileIdle = null;
    private Byte whenExhaustedAction = null;

    private final JedisPoolFactory jedisPoolFactory;
    private JedisPool jedisPool = null;

    private SourceCounter sourceCounter = null;

    public RedisSource() {
        jedisPoolFactory = new JedisPoolFactoryImpl();
    }

    @VisibleForTesting
    public RedisSource(JedisPoolFactory _jedisPoolFactory) {
        if (_jedisPoolFactory == null) {
            throw new IllegalArgumentException("JedisPoolFactory cannot be null");
        }

        this.jedisPoolFactory = _jedisPoolFactory;
    }

    @Override
    protected Status doProcess() throws EventDeliveryException {
        Status status = Status.READY;

        try {
            List<Event> failedEvents = previousFailedEvents.get();
            if (CollectionUtils.isEmpty(failedEvents)) {
                status = doRegularProcess();
            } else {
                status = doFailedEventsProcess();
            }
        } catch (Throwable t) {
            status = Status.BACKOFF;
            logger.error("Error receiving events", t);
        }

        return status;
    }

    private Status doFailedEventsProcess() throws EventDeliveryException {
        Status status = Status.READY;

        try {
            List<Event> failedEvents = previousFailedEvents.get();
            if (CollectionUtils.isNotEmpty(failedEvents)) {
                ChannelProcessor channelProcessor = getChannelProcessor();
                channelProcessor.processEventBatch(failedEvents);

                sourceCounter.addToEventAcceptedCount(failedEvents.size());
                sourceCounter.incrementAppendBatchAcceptedCount();
            }
            previousFailedEvents.remove();
        } catch (Throwable t) {
            status = Status.BACKOFF;
            logger.error("Error processing previously failing events", t);
        }

        return status;
    }

    private Status doRegularProcess() throws EventDeliveryException {
        Status status = Status.READY;

        Jedis jedis = null;
        List<Event> flumeEvents = new ArrayList<Event>(batchSize);
        try {
            jedis = jedisPool.getResource();
            Transaction jedisTransaction = jedis.multi();
            Response<List<byte[]>> responseEventsSource = jedisTransaction.lrange(redisKey, 0, batchSize);
            jedisTransaction.ltrim(redisKey, batchSize, -1);
            jedisTransaction.exec();

            List<byte[]> eventsSource = responseEventsSource.get();

            if (CollectionUtils.isEmpty(eventsSource)) {
                status = Status.BACKOFF;
            } else {

                sourceCounter.addToEventReceivedCount(eventsSource.size());
                sourceCounter.incrementAppendBatchReceivedCount();

                for (byte[] eventSource : eventsSource) {
                    try {
                        Event flumeEvent = serializer.parseEvent(eventSource);
                        
                        if(flumeEvent != null && logger.isTraceEnabled()) {
                        	logger.trace("Event parsed as :" + flumeEvent.toString());
                        }
                        
                        flumeEvents.add(flumeEvent);
                    } catch (RedisSerializerException e) {
                        logger.error("Could not parse event [" + new String(eventSource) + "]", e);
                    }
                }

                ChannelProcessor channelProcessor = getChannelProcessor();
                channelProcessor.processEventBatch(flumeEvents);

                sourceCounter.addToEventAcceptedCount(eventsSource.size());
                sourceCounter.incrementAppendBatchAcceptedCount();
            }
        } catch (JedisConnectionException e) {
            jedisPool.returnBrokenResource(jedis);
            logger.error("Error while receiving events from redis", e);
        } catch (ChannelException e) {
            if (CollectionUtils.isNotEmpty(flumeEvents)) {
                previousFailedEvents.set(flumeEvents);
            }
            status = Status.BACKOFF;
            logger.error("Error receiving events. Will retry same batch", e);

        } catch (Throwable t) {
            status = Status.BACKOFF;
            logger.error("Error receiving events", t);
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }

        return status;
    }

    @Override
    protected void doConfigure(Context context) throws FlumeException {
        logger.info("Configuring");
        host = context.getString(RedisSourceConfigurationConstant.HOST);
        Preconditions.checkState(StringUtils.isNotBlank(host),
                "host cannot be empty, please specify in configuration file");

        port = context.getInteger(RedisSourceConfigurationConstant.PORT, Protocol.DEFAULT_PORT);
        timeout = context.getInteger(RedisSourceConfigurationConstant.TIMEOUT, Protocol.DEFAULT_TIMEOUT);
        database = context.getInteger(RedisSourceConfigurationConstant.DATABASE, Protocol.DEFAULT_DATABASE);
        password = context.getString(RedisSourceConfigurationConstant.PASSWORD);
        redisKey = context
                .getString(RedisSourceConfigurationConstant.KEY, RedisSourceConfigurationConstant.DEFAULT_KEY)
                .getBytes();
        batchSize = context.getInteger(RedisSourceConfigurationConstant.BATCH_SIZE,
                RedisSourceConfigurationConstant.DEFAULT_BATCH_SIZE);
        String serializerClassName = context.getString(RedisSourceConfigurationConstant.SERIALIZER,
                RedisSourceConfigurationConstant.DEFAULT_SERIALIZER_CLASS_NAME);

        maxActive = context.getInteger(RedisSourceConfigurationConstant.REDIS_MAX_ACTIVE);
        maxIdle = context.getInteger(RedisSourceConfigurationConstant.REDIS_MAX_IDLE);
        minIdle = context.getInteger(RedisSourceConfigurationConstant.REDIS_MIN_IDLE);
        numTestsPerEvictionRun = context.getInteger(RedisSourceConfigurationConstant.REDIS_NUM_TESTS_PER_EVICTION_RUN);
        maxWait = context.getLong(RedisSourceConfigurationConstant.REDIS_MAX_WAIT);
        minEvictableIdleTimeMillis = context
                .getLong(RedisSourceConfigurationConstant.REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS);
        softMinEvictableIdleTimeMillis = context
                .getLong(RedisSourceConfigurationConstant.REDIS_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
        testOnBorrow = context.getBoolean(RedisSourceConfigurationConstant.REDIS_TEST_ON_BORROW);
        testOnReturn = context.getBoolean(RedisSourceConfigurationConstant.REDIS_TEST_ON_RETURN);
        testWhileIdle = context.getBoolean(RedisSourceConfigurationConstant.REDIS_TEST_WHILE_IDLE);
        timeBetweenEvictionRunsMillis = context
                .getLong(RedisSourceConfigurationConstant.REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS);

        String whenExhaustedActionStr = context.getString(RedisSourceConfigurationConstant.REDIS_WHEN_EXHAUSTED_ACTION);
        if (StringUtils.isNotBlank(whenExhaustedActionStr) == true) {
            if (whenExhaustedActionStr.equalsIgnoreCase(WHEN_EXHAUSTED_BLOCK)) {
                whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
            } else if (whenExhaustedActionStr.equalsIgnoreCase(WHEN_EXHAUSTED_FAIL)) {
                whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_FAIL;
            } else if (whenExhaustedActionStr.equalsIgnoreCase(WHEN_EXHAUSTED_GROW)) {
                whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_GROW;
            }
        }

        Preconditions.checkState(batchSize > 0, RedisSourceConfigurationConstant.BATCH_SIZE
                + " parameter must be greater than 1");

        try {
            /**
             * Instantiate serializer
             */
            @SuppressWarnings("unchecked") Class<? extends Serializer> clazz = (Class<? extends Serializer>) Class
                    .forName(serializerClassName);
            serializer = clazz.newInstance();

            /**
             * Configure it
             */
            Context serializerContext = new Context();
            serializerContext.putAll(context.getSubProperties(RedisSourceConfigurationConstant.SERIALIZER_PREFIX));
            serializer.configure(serializerContext);

        } catch (ClassNotFoundException e) {
            logger.error("Could not instantiate event serializer", e);
            Throwables.propagate(e);
        } catch (InstantiationException e) {
            logger.error("Could not instantiate event serializer", e);
            Throwables.propagate(e);
        } catch (IllegalAccessException e) {
            logger.error("Could not instantiate event serializer", e);
            Throwables.propagate(e);
        }

        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }

    }

    @Override
    protected void doStart() throws FlumeException {
        logger.info("Starting");
        if (jedisPool != null) {
            jedisPool.destroy();
        }

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        if (maxActive != null) {
            jedisPoolConfig.setMaxActive(maxActive);
        }
        if (maxIdle != null) {
            jedisPoolConfig.setMaxIdle(maxIdle);
        }
        if (maxWait != null) {
            jedisPoolConfig.setMaxWait(maxWait);
        }
        if (minEvictableIdleTimeMillis != null) {
            jedisPoolConfig.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
        }
        if (minIdle != null) {
            jedisPoolConfig.setMinIdle(minIdle);
        }
        if (numTestsPerEvictionRun != null) {
            jedisPoolConfig.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
        }
        if (softMinEvictableIdleTimeMillis != null) {
            jedisPoolConfig.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);
        }
        if (testOnBorrow != null) {
            jedisPoolConfig.setTestOnBorrow(testOnBorrow);
        }
        if (testOnReturn != null) {
            jedisPoolConfig.setTestOnReturn(testOnReturn);
        }
        if (testWhileIdle != null) {
            jedisPoolConfig.setTestWhileIdle(testWhileIdle);
        }
        if (timeBetweenEvictionRunsMillis != null) {
            jedisPoolConfig.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        }
        if (whenExhaustedAction != null) {
            jedisPoolConfig.setWhenExhaustedAction(whenExhaustedAction);
        }

        jedisPool = jedisPoolFactory.create(jedisPoolConfig, host, port, timeout, password, database);

        sourceCounter.start();
    }

    @Override
    protected void doStop() throws FlumeException {
        logger.info("Stoping");

        if (jedisPool != null) {
            jedisPool.destroy();
        }

        sourceCounter.stop();
    }

}
