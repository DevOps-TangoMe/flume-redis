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

import com.tango.logstash.flume.redis.sink.serializer.PlainSerializer;

public class RedisSinkConfigurationConstant {

	public static final String HOST = "host";
	public static final String PORT = "port";
	public static final String TIMEOUT = "timeout";
	public static final String PASSWORD = "password";
	public static final String DATABASE = "database";
	public static final String KEY = "key";
	public static final String BATCH_SIZE = "batch_size";
	public static final String SERIALIZER = "serializer";
	public static final String SERIALIZER_PREFIX = "serializer.";

	public static final String DEFAULT_KEY = "flume";
	public static final Integer DEFAULT_BATCH_SIZE = 1;
	public static final String DEFAULT_SERIALIZER_CLASS_NAME = PlainSerializer.class.getName();
}
