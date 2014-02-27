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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.tango.logstash.flume.redis.core.redis.JedisPoolFactory;
import com.tango.logstash.flume.redis.core.redis.JedisPoolFactoryImpl;
import com.tango.logstash.flume.redis.sink.serializer.RedisSerializerException;
import com.tango.logstash.flume.redis.sink.serializer.Serializer;

/*
 * Simple sink which read events from a channel and lpush them to redis
 */
public class RedisSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(RedisSink.class);

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

    private final JedisPoolFactory jedisPoolFactory;
    private JedisPool jedisPool = null;

    public RedisSink() {
        jedisPoolFactory = new JedisPoolFactoryImpl();
    }

    @VisibleForTesting
    public RedisSink(JedisPoolFactory _jedisPoolFactory) {
        if (_jedisPoolFactory == null) {
            throw new IllegalArgumentException("JedisPoolFactory cannot be null");
        }

        this.jedisPoolFactory = _jedisPoolFactory;
    }

    @Override
    public synchronized void start() {
        logger.info("Starting");
        if (jedisPool != null) {
            jedisPool.destroy();
        }

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPool = jedisPoolFactory.create(jedisPoolConfig, host, port, timeout, password, database);

        super.start();
    }

    @Override
    public synchronized void stop() {
        logger.info("Stoping");

        if (jedisPool != null) {
            jedisPool.destroy();
        }
        super.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;

        if (jedisPool == null) {
            throw new EventDeliveryException("Redis connection not established. Please verify your configuration");
        }

        List<byte[]> batchEvents = new ArrayList<byte[]>(batchSize);

        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();
        Jedis jedis = jedisPool.getResource();
        try {
            txn.begin();

            for (int i = 0; i < batchSize && status != Status.BACKOFF; i++) {
                Event event = channel.take();
                if (event == null) {
                    status = Status.BACKOFF;
                } else {
                    try {
                        batchEvents.add(serializer.serialize(event));
                    } catch (RedisSerializerException e) {
                        logger.error("Could not serialize event " + event, e);
                    }
                }
            }

            /**
             * Only send events if we got any
             */
            if (batchEvents.size() > 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Sending " + batchEvents.size() + " events");
                }

                byte[][] redisEvents = new byte[batchEvents.size()][];
                int index = 0;
                for (byte[] redisEvent : batchEvents) {
                    redisEvents[index] = redisEvent;
                    index++;
                }

                jedis.lpush(redisKey, redisEvents);

            }

            txn.commit();
        } catch (JedisConnectionException e) {
            txn.rollback();
            jedisPool.returnBrokenResource(jedis);
            logger.error("Error while shipping events to redis", e);
        } catch (Throwable t) {
            txn.rollback();
            logger.error("Unexpected error", t);
        } finally {
            txn.close();
            jedisPool.returnResource(jedis);
        }

        return status;
    }

    @Override
    public void configure(Context context) {
        logger.info("Configuring");
        host = context.getString(RedisSinkConfigurationConstant.HOST);
        Preconditions.checkState(StringUtils.isNotBlank(host),
                "host cannot be empty, please specify in configuration file");

        port = context.getInteger(RedisSinkConfigurationConstant.PORT, Protocol.DEFAULT_PORT);
        timeout = context.getInteger(RedisSinkConfigurationConstant.TIMEOUT, Protocol.DEFAULT_TIMEOUT);
        database = context.getInteger(RedisSinkConfigurationConstant.DATABASE, Protocol.DEFAULT_DATABASE);
        password = context.getString(RedisSinkConfigurationConstant.PASSWORD);
        redisKey = context.getString(RedisSinkConfigurationConstant.KEY, RedisSinkConfigurationConstant.DEFAULT_KEY)
                .getBytes();
        batchSize = context.getInteger(RedisSinkConfigurationConstant.BATCH_SIZE,
                RedisSinkConfigurationConstant.DEFAULT_BATCH_SIZE);
        String serializerClassName = context.getString(RedisSinkConfigurationConstant.SERIALIZER,
                RedisSinkConfigurationConstant.DEFAULT_SERIALIZER_CLASS_NAME);

        Preconditions.checkState(batchSize > 0, RedisSinkConfigurationConstant.BATCH_SIZE
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
            serializerContext.putAll(context.getSubProperties(RedisSinkConfigurationConstant.SERIALIZER_PREFIX));
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

    }
}
