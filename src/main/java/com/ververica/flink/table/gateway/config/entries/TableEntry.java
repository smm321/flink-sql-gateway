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
import com.ververica.flink.table.gateway.utils.SqlGatewayException;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;
import java.util.Map;

/**
 * Describes a table-like configuration entry.
 */
public abstract class TableEntry extends ConfigEntry {

    public static final String TABLES_NAME = "name";

    private static final String TABLES_TYPE = "type";

    @Deprecated
    private static final String TABLES_TYPE_VALUE_SOURCE = "source";

    @Deprecated
    private static final String TABLES_TYPE_VALUE_SINK = "sink";

    @Deprecated
    private static final String TABLES_TYPE_VALUE_BOTH = "both";

    private static final String TABLES_TYPE_VALUE_SOURCE_TABLE = "source-table";

    private static final String TABLES_TYPE_VALUE_SINK_TABLE = "sink-table";

    private static final String TABLES_TYPE_VALUE_SOURCE_SINK_TABLE = "source-sink-table";

    private static final String TABLES_TYPE_VALUE_VIEW = "view";

    private static final String TABLES_TYPE_VALUE_TEMPORAL_TABLE = "temporal-table";

    private final String name;

    protected TableEntry(String name, Configuration properties) {
        super(properties);
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static TableEntry create(Map<String, Object> config) {
        return create(ConfigUtil.normalizeYaml(config));
    }

    private static TableEntry create(Configuration properties) {
        ConfigurationValidater validate = ConfigurationValidater.builder().configuration(properties).build();
        validate.validateString(TABLES_NAME, false, 1);
        validate.validateEnumValues(
                TABLES_TYPE,
                false,
                Arrays.asList(
                        TABLES_TYPE_VALUE_SOURCE,
                        TABLES_TYPE_VALUE_SOURCE_TABLE,
                        TABLES_TYPE_VALUE_SINK,
                        TABLES_TYPE_VALUE_SINK_TABLE,
                        TABLES_TYPE_VALUE_BOTH,
                        TABLES_TYPE_VALUE_SOURCE_SINK_TABLE,
                        TABLES_TYPE_VALUE_VIEW,
                        TABLES_TYPE_VALUE_TEMPORAL_TABLE));

        final String name = properties.getString(ConfigOptions.key(TABLES_NAME).stringType().noDefaultValue());

        final Configuration cleanedProperties =
                validate.withoutKeys(Arrays.asList(TABLES_NAME, TABLES_TYPE));

        switch (properties.getString(ConfigOptions.key(TABLES_TYPE).stringType().noDefaultValue())) {
            case TABLES_TYPE_VALUE_SOURCE:
            case TABLES_TYPE_VALUE_SOURCE_TABLE:
                return new SourceTableEntry(name, cleanedProperties);
            case TABLES_TYPE_VALUE_SINK:
            case TABLES_TYPE_VALUE_SINK_TABLE:
                return new SinkTableEntry(name, cleanedProperties);
            case TABLES_TYPE_VALUE_BOTH:
            case TABLES_TYPE_VALUE_SOURCE_SINK_TABLE:
                return new SourceSinkTableEntry(name, cleanedProperties);
            case TABLES_TYPE_VALUE_VIEW:
                return new ViewEntry(name, cleanedProperties);
            case TABLES_TYPE_VALUE_TEMPORAL_TABLE:
                return new TemporalTableEntry(name, cleanedProperties);
            default:
                throw new SqlGatewayException("Unexpected table type.");
        }
    }
}
