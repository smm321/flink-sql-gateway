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
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.util.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Describes a catalog configuration entry.
 */
public class CatalogEntry extends ConfigEntry {

    public static final String CATALOG_NAME = "name";

    private final String name;

    protected CatalogEntry(String name, Configuration properties) {
        super(properties);
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    protected void validate(Configuration properties) {
//        ConfigurationValidater validater = ConfigurationValidater.builder().configuration(properties).build();
//        validater.validateString(CATALOG_NAME, false,1);
    }

    public static CatalogEntry create(Map<String, Object> config) {
        return create(ConfigUtil.normalizeYaml(config));
    }

    private static CatalogEntry create(Configuration properties) {
        final String name = properties.getString(ConfigOptions.key(CATALOG_NAME).stringType().noDefaultValue());
        properties.removeConfig(ConfigOptions.key(CATALOG_NAME).stringType().noDefaultValue());
        return new CatalogEntry(name, Configuration.fromMap(properties.toMap()));
    }
}
