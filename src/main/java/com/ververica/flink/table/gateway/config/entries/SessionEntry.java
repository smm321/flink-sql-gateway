/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flink.table.gateway.config.entries;

import com.ververica.flink.table.gateway.config.ConfigUtil;
import com.ververica.flink.table.gateway.utils.ConfigurationValidater;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.TimeUtils;

import java.util.HashMap;
import java.util.Map;

import static com.ververica.flink.table.gateway.config.Environment.SESSION_ENTRY;

/**
 * Describes a session configuration entry.
 */
public class SessionEntry extends ConfigEntry {

    public static final SessionEntry DEFAULT_INSTANCE = new SessionEntry(new Configuration());

    private static final String SESSION_IDLE_TIMEOUT = "idle-timeout";

    private static final String SESSION_CHECK_INTERVAL = "check-interval";

    private static final String SESSION_MAX_COUNT = "max-count";

    private SessionEntry(Configuration properties) {
        super(properties);
    }

    @Override
    protected void validate(Configuration properties) {
        ConfigurationValidater validater = ConfigurationValidater.builder().configuration(properties).build();
        validater.validateDuration(SESSION_IDLE_TIMEOUT, true, 1);
        validater.validateDuration(SESSION_CHECK_INTERVAL, true, 1);
        validater.validateLong(SESSION_MAX_COUNT, true, 0);
    }

    public static SessionEntry create(Map<String, Object> config) {
        return new SessionEntry(ConfigUtil.normalizeYaml(config));
    }

    public Map<String, String> asTopLevelMap() {
        ConfigurationValidater validater = ConfigurationValidater.builder().configuration(configuration).build();
        return validater.asPrefixedMap(SESSION_ENTRY + '.');
    }

    /**
     * Merges two session entries. The properties of the first execution entry might be
     * overwritten by the second one.
     */
    public static SessionEntry merge(SessionEntry session1, SessionEntry session2) {
        final Map<String, String> mergedProperties = new HashMap<>(session1.asTopLevelMap());
        mergedProperties.putAll(session2.asTopLevelMap());
        return new SessionEntry(Configuration.fromMap(mergedProperties));
    }

    public long getIdleTimeout() {
        String timeout = configuration.getOptional(
                ConfigOptions.key(SESSION_IDLE_TIMEOUT).stringType().defaultValue("1d")).get();
        return TimeUtils.parseDuration(timeout).toMillis();
    }

    public long getCheckInterval() {
        String interval = configuration.getOptional(
                ConfigOptions.key(SESSION_CHECK_INTERVAL).stringType().defaultValue("1h")).get();
        return TimeUtils.parseDuration(interval).toMillis();
    }

    public long getMaxCount() {
        return configuration.getOptional(
                ConfigOptions.key(SESSION_MAX_COUNT).longType().defaultValue(1000000L)).get();
    }
}
