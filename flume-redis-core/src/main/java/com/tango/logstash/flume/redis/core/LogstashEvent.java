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
package com.tango.logstash.flume.redis.core;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;

import com.tango.logstash.flume.redis.core.LogstashConstant;

/**
 * For now, parsing a logstash event through accepting multiple kind of attributes. Eventually it would be nice to have
 * different parsing strategies depending on the logstash format version
 * 
 */
@JsonSerialize(include = Inclusion.NON_NULL)
public class LogstashEvent {

    /**
     * Message - Complete event body
     */
    private String message;

    @JsonProperty(LogstashConstant.ATTRIBUTE_AT_MESSAGE)
    public void setAtMessage(String _message) {
        this.message = _message;
    }

    @JsonProperty(LogstashConstant.ATTRIBUTE_MESSAGE)
    public void setMessage(String _message) {
        this.message = _message;
    }

    public String getMessage() {
        return message;
    }

    /**
     * User defined fields
     */
    @JsonProperty(LogstashConstant.ATTRIBUTE_AT_FIELDS)
    private Map<String, String> fields = new HashMap<String, String>();

    public void putField(String name, String value) {
        fields.put(name, value);
    }

    public Map<String, String> getFields() {
        return fields;
    }

    /**
     * Timestamp
     */
    @JsonProperty(LogstashConstant.ATTRIBUTE_AT_TIMESTAMP)
    private Date timestamp;

    public void setTimestamp(Date _timestamp) {
        this.timestamp = _timestamp;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    /**
     * Source of the event
     */
    @JsonProperty(LogstashConstant.ATTRIBUTE_SOURCE)
    private String source;

    public void setSource(String _source) {
        this.source = _source;
    }

    public String getSource() {
        return source;
    }

    /**
     * Host which has emitted this event
     */
    private String sourceHost;

    @JsonProperty(LogstashConstant.ATTRIBUTE_SOURCE_HOST)
    public void setAtSourceHost(String _sourceHost) {
        this.sourceHost = _sourceHost;
    }

    @JsonProperty(LogstashConstant.ATTRIBUTE_HOST)
    public void setSourceHost(String _sourceHost) {
        this.sourceHost = _sourceHost;
    }

    public String getSourceHost() {
        return sourceHost;
    }

    /**
     * Source path
     */
    private String sourcePath;

    @JsonProperty(LogstashConstant.ATTRIBUTE_SOURCE_PATH)
    public void setAtSourcePath(String _sourcePath) {
        this.sourcePath = _sourcePath;
    }

    @JsonProperty(LogstashConstant.ATTRIBUTE_PATH)
    public void setSourcePath(String _sourcePath) {
        this.sourcePath = _sourcePath;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    /**
     * Type
     */
    private String type;

    @JsonProperty(LogstashConstant.ATTRIBUTE_TYPE)
    public void setAtType(String _type) {
        this.type = _type;
    }

    @JsonProperty("type")
    public void setType(String _type) {
        this.type = _type;
    }

    public String getType() {
        return type;
    }

    /**
     * Logstsah format version
     */
    @JsonProperty(LogstashConstant.ATTRIBUTE_AT_VERSION)
    private Integer version;

    public Integer getVersion() {
        return version;
    }

    /**
     * Tags associated with this event
     */
    private List<String> tags;

    @JsonProperty(LogstashConstant.ATTRIBUTE_AT_TAGS)
    public void setAtTags(List<String> _tags) {
        this.tags = _tags;
    }

    @JsonProperty(LogstashConstant.ATTRIBUTE_TAGS)
    public void setTags(List<String> _tags) {
        this.tags = _tags;
    }

    public List<String> getTags() {
        return tags;
    }

}
