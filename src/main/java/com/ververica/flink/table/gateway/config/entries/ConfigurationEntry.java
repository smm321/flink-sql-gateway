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
import org.apache.flink.configuration.Configuration;

import java.util.HashMap;
import java.util.Map;

import static com.ververica.flink.table.gateway.config.Environment.CONFIGURATION_ENTRY;

/**
 * Configuration for configuring {@link org.apache.flink.table.api.TableConfig}.
 */
public class ConfigurationEntry extends ConfigEntry {

    public static final ConfigurationEntry DEFAULT_INSTANCE =
            new ConfigurationEntry(new Configuration());

    private ConfigurationEntry(Configuration properties) {
        super(properties);
    }

    @Override
    protected void validate(Configuration properties) {
        // Nothing to validate as the planner will check the options
    }

    // --------------------------------------------------------------------------------------------

    public static ConfigurationEntry create(Map<String, Object> config) {
        return new ConfigurationEntry(ConfigUtil.normalizeYaml(config));
    }

    /**
     * Merges two configuration entries. The properties of the first configuration entry might be
     * overwritten by the second one.
     */
    public static ConfigurationEntry merge(ConfigurationEntry configuration1, ConfigurationEntry configuration2) {
        final Map<String, String> mergedProperties = new HashMap<>(configuration1.asMap());
        mergedProperties.putAll(configuration2.asMap());

        final Configuration configuration = new Configuration();
        mergedProperties.forEach((k, v) -> {
            configuration.setString(k, v);
        });

        return new ConfigurationEntry(configuration);
    }

    public static ConfigurationEntry enrich(ConfigurationEntry configuration, Map<String, String> prefixedProperties) {
        final Map<String, String> enrichedProperties = new HashMap<>(configuration.asMap());

        prefixedProperties.forEach((k, v) -> {
            final String normalizedKey = k.toLowerCase();
            if (k.startsWith(CONFIGURATION_ENTRY + ".")) {
                enrichedProperties.put(normalizedKey, v);
            }
        });

        final Configuration enrichConfiguration = new Configuration();
        enrichedProperties.forEach((k, v) -> {
            enrichConfiguration.setString(k, v);
        });

        return new ConfigurationEntry(enrichConfiguration);
    }
}
