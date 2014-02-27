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

import com.tango.logstash.flume.redis.source.serializer.PlainDeSerializer;

public class RedisSourceConfigurationConstant {

    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String TIMEOUT = "timeout";
    public static final String PASSWORD = "password";
    public static final String DATABASE = "database";
    public static final String KEY = "key";
    public static final String BATCH_SIZE = "batch_size";
    public static final String SERIALIZER = "serializer";
    public static final String SERIALIZER_PREFIX = "serializer.";
    public static final String REDIS_MAX_ACTIVE = "redis_max_active";
    public static final String REDIS_MAX_IDLE = "redis_max_idle";
    public static final String REDIS_MIN_IDLE = "redis_min_idle";
    public static final String REDIS_MAX_WAIT = "redis_max_wait";
    public static final String REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS = "redis_minEvictableIdleTimeMillis";
    public static final String REDIS_NUM_TESTS_PER_EVICTION_RUN = "redis_numTestsPerEvictionRun";
    public static final String REDIS_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS = "redis_softMinEvictableIdleTimeMillis";
    public static final String REDIS_TEST_ON_BORROW = "redis_test_on_borrow";
    public static final String REDIS_TEST_ON_RETURN = "redis_test_on_return";
    public static final String REDIS_TEST_WHILE_IDLE = "redis_test_while_idle";
    public static final String REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS = "redis_timeBetweenEvictionRunsMillis";
    public static final String REDIS_WHEN_EXHAUSTED_ACTION = "redis_whenExhaustedAction";

    public static final String DEFAULT_KEY = "flume";
    public static final Integer DEFAULT_BATCH_SIZE = 1;
    public static final String DEFAULT_SERIALIZER_CLASS_NAME = PlainDeSerializer.class.getName();
}
