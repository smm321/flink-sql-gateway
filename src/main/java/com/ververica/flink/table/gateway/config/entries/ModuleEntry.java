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

import java.util.Collections;
import java.util.Map;

/**
 * Describes a module configuration entry.
 */
public class ModuleEntry extends ConfigEntry {

    public static final String MODULE_NAME = "name";

    public static final String MODULE_TYPE = "type";

    private final String name;

    protected ModuleEntry(String name, Configuration properties) {
        super(properties);
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    protected void validate(Configuration properties) {
        ConfigurationValidater validate = ConfigurationValidater.builder().configuration(properties).build();
        validate.validateString(MODULE_TYPE, false, 1);
    }

    public static ModuleEntry create(Map<String, Object> config) {
        return create(ConfigUtil.normalizeYaml(config));
    }

    private static ModuleEntry create(Configuration properties) {
        ConfigurationValidater validate = ConfigurationValidater.builder().configuration(properties).build();
        validate.validateString(MODULE_TYPE, false, 1);
        final String name = properties.get(ConfigOptions.key(MODULE_NAME).stringType().noDefaultValue());
        final Configuration cleanedProperties = validate.withoutKeys(Collections.singletonList(MODULE_NAME));
        return new ModuleEntry(name, cleanedProperties);
    }
}
