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
package com.tango.logstash.flume.redis.source.serializer;

public class RedisSerializerException extends Exception {

    private static final long serialVersionUID = 4803787253278382398L;

	public RedisSerializerException() {
      super();
    }

    public RedisSerializerException(String message) {
      super(message);
    }

    public RedisSerializerException(String message, Throwable t) {
      super(message, t);
    }

    public RedisSerializerException(Throwable t) {
      super(t);
    }

}
