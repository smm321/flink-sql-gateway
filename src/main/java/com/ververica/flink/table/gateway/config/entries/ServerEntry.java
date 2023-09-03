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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.ververica.flink.table.gateway.config.Environment.SERVER_ENTRY;

/**
 * Describes a gateway configuration entry.
 */
public class ServerEntry extends ConfigEntry {

    public static final ServerEntry DEFAULT_INSTANCE = new ServerEntry(new Configuration());

    private static final String DEFAULT_ADDRESS = "127.0.0.1";

    private static final int DEFAULT_PORT = 8083;

    private static final String GATEWAY_BIND_ADDRESS = "bind-address";

    private static final String GATEWAY_ADDRESS = "address";

    private static final String GATEWAY_PORT = "port";

    private static final String JVM_ARGS = "jvm_args";

    private ServerEntry(Configuration properties) {
        super(properties);
    }

    @Override
    protected void validate(Configuration properties) {
        ConfigurationValidater validate = ConfigurationValidater.builder().configuration(properties).build();
        validate.validateString(GATEWAY_BIND_ADDRESS, true);
        validate.validateString(GATEWAY_ADDRESS, true);
        validate.validateInt(GATEWAY_PORT, true, 1024, 65535);
        validate.validateString(JVM_ARGS, true);
    }

    public static ServerEntry create(Map<String, Object> config) {
        return new ServerEntry(ConfigUtil.normalizeYaml(config));
    }

    public Map<String, String> asTopLevelMap() {
        ConfigurationValidater validater = ConfigurationValidater.builder().configuration(configuration).build();
        return validater.asPrefixedMap(SERVER_ENTRY + '.');
    }

    /**
     * Merges two session entries. The properties of the first execution entry might be
     * overwritten by the second one.
     */
    public static ServerEntry merge(ServerEntry gateway1, ServerEntry gateway2) {
        final Map<String, String> mergedProperties = new HashMap<>(gateway1.asTopLevelMap());
        mergedProperties.putAll(gateway2.asTopLevelMap());
        return new ServerEntry(Configuration.fromMap(mergedProperties));
    }

    public Optional<String> getBindAddress() {
        return configuration.getOptional(ConfigOptions.key(GATEWAY_BIND_ADDRESS).stringType().noDefaultValue());
    }

    public String getAddress() {
        return configuration.getOptional(ConfigOptions
                .key(GATEWAY_ADDRESS)
                .stringType()
                .defaultValue(DEFAULT_ADDRESS))
                .get();
    }

    public int getPort() {
        return configuration.getOptional(ConfigOptions
                .key(GATEWAY_PORT)
                .intType()
                .defaultValue(DEFAULT_PORT))
                .get();
    }

    public String getJvmArgs() {
        return configuration.getOptional(ConfigOptions
                .key(JVM_ARGS)
                .stringType()
                .defaultValue(""))
                .get();
    }
}
