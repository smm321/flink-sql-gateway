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
import org.apache.flink.configuration.Configuration;

import java.util.List;

/**
 * Configuration of a temporal table.
 * TODO remove this.
 */
public class TemporalTableEntry extends TableEntry {

    private static final String TABLES_HISTORY_TABLE = "history-table";

    private static final String TABLES_PRIMARY_KEY = "primary-key";

    private static final String TABLES_TIME_ATTRIBUTE = "time-attribute";

    private final String historyTable;

    private final List<String> primaryKeyFields;

    private final String timeAttribute;

    TemporalTableEntry(String name, Configuration properties) {
        super(name, properties);
        ConfigurationValidater validate = ConfigurationValidater.builder().configuration(configuration).build();
        historyTable = validate.getString(TABLES_HISTORY_TABLE);
        primaryKeyFields = validate.getArray(TABLES_PRIMARY_KEY, validate::getString);
        timeAttribute = validate.getString(TABLES_TIME_ATTRIBUTE);
    }

    public String getHistoryTable() {
        return historyTable;
    }

    public List<String> getPrimaryKeyFields() {
        return primaryKeyFields;
    }

    public String getTimeAttribute() {
        return timeAttribute;
    }

    @Override
    protected void validate(Configuration properties) {
        ConfigurationValidater validater = ConfigurationValidater.builder().configuration(properties).build();
        validater.validateString(TABLES_HISTORY_TABLE, false, 1);
        validater.validateArray(
                TABLES_PRIMARY_KEY,
                (key) -> validater.validateString(key, false, 1),
                1,
                1); // currently, composite primary keys are not supported
        validater.validateString(TABLES_TIME_ATTRIBUTE, false, 1);
    }
}
