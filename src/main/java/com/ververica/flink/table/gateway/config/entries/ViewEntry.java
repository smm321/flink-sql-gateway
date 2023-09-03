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

import com.ververica.flink.table.gateway.utils.ConfigurationValidater;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

/**
 * Configuration of a table view.
 */
public class ViewEntry extends TableEntry {

    private static final String TABLES_QUERY = "query";

    private final String query;

    ViewEntry(String name, Configuration properties) {
        super(name, properties);
        query = properties.getString(ConfigOptions.key(TABLES_QUERY).stringType().noDefaultValue());
    }

    public String getQuery() {
        return query;
    }

    @Override
    protected void validate(Configuration properties) {
        ConfigurationValidater validate = ConfigurationValidater.builder().configuration(properties).build();
        validate.validateString(TABLES_QUERY, false, 1);
    }

    public static ViewEntry create(String name, String query) {
        final Configuration properties = new Configuration();
        properties.setString(TABLES_QUERY, query);
        return new ViewEntry(name, properties);
    }
}
