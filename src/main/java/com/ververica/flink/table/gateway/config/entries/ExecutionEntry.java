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
import com.ververica.flink.table.gateway.config.Environment;
import com.ververica.flink.table.gateway.utils.ConfigurationValidater;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.table.api.EnvironmentSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.ververica.flink.table.gateway.config.Environment.EXECUTION_ENTRY;

/**
 * Configuration of a table program execution. This class parses the `execution` part
 * in an environment file. The execution describes properties that would usually be defined in the
 * ExecutionEnvironment/StreamExecutionEnvironment/TableEnvironment or as code in a Flink job.
 *
 * <p>All properties of this entry are optional and evaluated lazily.
 * TODO remove the useless properties
 */
public class ExecutionEntry extends ConfigEntry {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutionEntry.class);

    public static final ExecutionEntry DEFAULT_INSTANCE =
            new ExecutionEntry(new Configuration());

    public static final String EXECUTION_PLANNER = "planner";

    public static final String EXECUTION_PLANNER_VALUE_BLINK = "blink";

    public static final String EXECUTION_TYPE = "type";

    public static final String EXECUTION_TYPE_VALUE_STREAMING = "streaming";

    public static final String EXECUTION_TYPE_VALUE_BATCH = "batch";

    private static final String EXECUTION_TIME_CHARACTERISTIC = "time-characteristic";

    private static final String EXECUTION_TIME_CHARACTERISTIC_VALUE_EVENT_TIME = "event-time";

    private static final String EXECUTION_TIME_CHARACTERISTIC_VALUE_PROCESSING_TIME = "processing-time";

    private static final String EXECUTION_PERIODIC_WATERMARKS_INTERVAL = "periodic-watermarks-interval";

    private static final String EXECUTION_MIN_STATE_RETENTION = "min-idle-state-retention";

    private static final String EXECUTION_MAX_STATE_RETENTION = "max-idle-state-retention";

    private static final String EXECUTION_PARALLELISM = "parallelism";

    private static final String EXECUTION_MAX_PARALLELISM = "max-parallelism";

    public static final String EXECUTION_RESULT_MODE = "result-mode";

    public static final String EXECUTION_RESULT_MODE_VALUE_CHANGELOG = "changelog";

    public static final String EXECUTION_RESULT_MODE_VALUE_TABLE = "table";

    public static final String EXECUTION_MAX_BUFFER_SIZE = "max_buffer_size";

    private static final String EXECUTION_RESTART_STRATEGY_TYPE = "restart-strategy.type";

    private static final String EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FALLBACK = "fallback";

    private static final String EXECUTION_RESTART_STRATEGY_TYPE_VALUE_NONE = "none";

    private static final String EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FIXED_DELAY = "fixed-delay";

    private static final String EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FAILURE_RATE = "failure-rate";

    private static final String EXECUTION_RESTART_STRATEGY_ATTEMPTS = "restart-strategy.attempts";

    private static final String EXECUTION_RESTART_STRATEGY_DELAY = "restart-strategy.delay";

    private static final String EXECUTION_RESTART_STRATEGY_FAILURE_RATE_INTERVAL = "restart-strategy.failure-rate-interval";

    private static final String EXECUTION_RESTART_STRATEGY_MAX_FAILURES_PER_INTERVAL = "restart-strategy.max-failures-per-interval";

    public static final String EXECUTION_CURRENT_CATALOG = "current-catalog";

    public static final String EXECUTION_CURRENT_DATABASE = "current-database";

    public static final String EXECUTION_CHECKPOINT_INTERVAL = "checkpoint-interval";

    private ExecutionEntry(Configuration properties) {
        super(properties);
    }

    @Override
    protected void validate(Configuration configuration) {
        ConfigurationValidater property = ConfigurationValidater.builder().configuration(configuration).build();
        property.validateEnumValues(
                EXECUTION_PLANNER,
                true,
                Arrays.asList(
                        EXECUTION_PLANNER_VALUE_BLINK));
        property.validateEnumValues(
                EXECUTION_TYPE,
                true,
                Arrays.asList(
                        EXECUTION_TYPE_VALUE_BATCH,
                        EXECUTION_TYPE_VALUE_STREAMING));
        property.validateEnumValues(
                EXECUTION_TIME_CHARACTERISTIC,
                true,
                Arrays.asList(
                        EXECUTION_TIME_CHARACTERISTIC_VALUE_EVENT_TIME,
                        EXECUTION_TIME_CHARACTERISTIC_VALUE_PROCESSING_TIME));
        property.validateLong(EXECUTION_PERIODIC_WATERMARKS_INTERVAL, true, 1);
        property.validateLong(EXECUTION_MIN_STATE_RETENTION, true, 0);
        property.validateLong(EXECUTION_MAX_STATE_RETENTION, true, 0);
        property.validateInt(EXECUTION_PARALLELISM, true, 1);
        property.validateInt(EXECUTION_MAX_PARALLELISM, true, 1);
        property.validateEnumValues(
                EXECUTION_RESTART_STRATEGY_TYPE,
                true,
                Arrays.asList(
                        EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FALLBACK,
                        EXECUTION_RESTART_STRATEGY_TYPE_VALUE_NONE,
                        EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FIXED_DELAY,
                        EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FAILURE_RATE));
        property.validateInt(EXECUTION_RESTART_STRATEGY_ATTEMPTS, true, 1);
        property.validateLong(EXECUTION_RESTART_STRATEGY_DELAY, true, 0);
        property.validateLong(EXECUTION_RESTART_STRATEGY_FAILURE_RATE_INTERVAL, true, 1);
        property.validateInt(EXECUTION_RESTART_STRATEGY_MAX_FAILURES_PER_INTERVAL, true, 1);
        property.validateString(EXECUTION_CURRENT_CATALOG, true, 1);
        property.validateString(EXECUTION_CURRENT_DATABASE, true, 1);
        property.validateInt(EXECUTION_MAX_BUFFER_SIZE, true, 1);
        property.validateInt(EXECUTION_CHECKPOINT_INTERVAL, true, 1);
    }

    public EnvironmentSettings getEnvironmentSettings() {
        final EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance();

        if (inStreamingMode()) {
            builder.inStreamingMode();
        } else if (inBatchMode()) {
            builder.inBatchMode();
        }

        return builder.build();
    }

    public boolean inStreamingMode() {
        ConfigurationValidater properties = ConfigurationValidater.builder().configuration(configuration).build();
        return properties.getOptionalString(EXECUTION_TYPE)
                .map((v) -> v.equals(EXECUTION_TYPE_VALUE_STREAMING))
                .orElse(false);
    }

    public boolean inBatchMode() {
        ConfigurationValidater properties = ConfigurationValidater.builder().configuration(configuration).build();
        return properties.getOptionalString(EXECUTION_TYPE)
                .map((v) -> v.equals(EXECUTION_TYPE_VALUE_BATCH))
                .orElse(false);
    }

    public boolean isStreamingPlanner() {
        return true;
    }

    public String getPlanner() {
        return EXECUTION_PLANNER_VALUE_BLINK;
    }

    public boolean isBatchPlanner() {
//        final String planner = properties.getOptionalString(EXECUTION_PLANNER)
//                .orElse(EXECUTION_PLANNER_VALUE_OLD);
//
//        // Blink planner is not a batch planner
//        if (planner.equals(EXECUTION_PLANNER_VALUE_BLINK)) {
//            return false;
//        }
//        // Old planner can be a streaming or batch planner
//        else if (planner.equals(EXECUTION_PLANNER_VALUE_OLD)) {
//            return inBatchMode();
//        }

        return false;
    }

    public TimeCharacteristic getTimeCharacteristic() {
        ConfigurationValidater properties = ConfigurationValidater.builder().configuration(configuration).build();
        return properties.getOptionalString(EXECUTION_TIME_CHARACTERISTIC)
                .flatMap((v) -> {
                    switch (v) {
                        case EXECUTION_TIME_CHARACTERISTIC_VALUE_EVENT_TIME:
                            return Optional.of(TimeCharacteristic.EventTime);
                        case EXECUTION_TIME_CHARACTERISTIC_VALUE_PROCESSING_TIME:
                            return Optional.of(TimeCharacteristic.ProcessingTime);
                        default:
                            return Optional.empty();
                    }
                })
                .orElseGet(() ->
                        useDefaultValue(
                                EXECUTION_TIME_CHARACTERISTIC,
                                TimeCharacteristic.EventTime,
                                EXECUTION_TIME_CHARACTERISTIC_VALUE_EVENT_TIME));
    }

    public long getPeriodicWatermarksInterval() {
        ConfigurationValidater properties = ConfigurationValidater.builder().configuration(configuration).build();
        return properties.getOptionalLong(EXECUTION_PERIODIC_WATERMARKS_INTERVAL)
                .orElseGet(() -> useDefaultValue(EXECUTION_PERIODIC_WATERMARKS_INTERVAL, 200L));
    }

    public long getMinStateRetention() {
        ConfigurationValidater properties = ConfigurationValidater.builder().configuration(configuration).build();
        return properties.getOptionalLong(EXECUTION_MIN_STATE_RETENTION)
                .orElseGet(() -> useDefaultValue(EXECUTION_MIN_STATE_RETENTION, 0L));
    }

    public long getMaxStateRetention() {
        ConfigurationValidater properties = ConfigurationValidater.builder().configuration(configuration).build();
        return properties.getOptionalLong(EXECUTION_MAX_STATE_RETENTION)
                .orElseGet(() -> useDefaultValue(EXECUTION_MAX_STATE_RETENTION, 0L));
    }

    public int getParallelism() {
        ConfigurationValidater properties = ConfigurationValidater.builder().configuration(configuration).build();
        return properties.getOptionalInt(EXECUTION_PARALLELISM)
                .orElseGet(() -> useDefaultValue(EXECUTION_PARALLELISM, 1));
    }

    public int getMaxParallelism() {
        ConfigurationValidater properties = ConfigurationValidater.builder().configuration(configuration).build();
        return properties.getOptionalInt(EXECUTION_MAX_PARALLELISM)
                .orElseGet(() -> useDefaultValue(EXECUTION_MAX_PARALLELISM, 128));
    }

    public int getCheckpointInterval() {
        ConfigurationValidater properties = ConfigurationValidater.builder().configuration(configuration).build();
        return properties.getOptionalInt(EXECUTION_CHECKPOINT_INTERVAL)
                .orElseGet(() -> useDefaultValue(EXECUTION_CHECKPOINT_INTERVAL, 0));
    }

    public RestartStrategies.RestartStrategyConfiguration getRestartStrategy() {
        ConfigurationValidater properties = ConfigurationValidater.builder().configuration(configuration).build();
        return properties.getOptionalString(EXECUTION_RESTART_STRATEGY_TYPE)
                .flatMap((v) -> {
                    switch (v) {
                        case EXECUTION_RESTART_STRATEGY_TYPE_VALUE_NONE:
                            return Optional.of(RestartStrategies.noRestart());
                        case EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FIXED_DELAY:
                            final int attempts = properties.getOptionalInt(EXECUTION_RESTART_STRATEGY_ATTEMPTS)
                                    .orElseGet(() -> useDefaultValue(EXECUTION_RESTART_STRATEGY_ATTEMPTS, Integer.MAX_VALUE));
                            final long fixedDelay = properties.getOptionalLong(EXECUTION_RESTART_STRATEGY_DELAY)
                                    .orElseGet(() -> useDefaultValue(EXECUTION_RESTART_STRATEGY_DELAY, 10_000L));
                            return Optional.of(RestartStrategies.fixedDelayRestart(attempts, fixedDelay));
                        case EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FAILURE_RATE:
                            final int failureRate = properties
                                    .getOptionalInt(EXECUTION_RESTART_STRATEGY_MAX_FAILURES_PER_INTERVAL)
                                    .orElseGet(() -> useDefaultValue(EXECUTION_RESTART_STRATEGY_MAX_FAILURES_PER_INTERVAL, 1));
                            final long failureInterval = properties
                                    .getOptionalLong(EXECUTION_RESTART_STRATEGY_FAILURE_RATE_INTERVAL)
                                    .orElseGet(
                                            () -> useDefaultValue(EXECUTION_RESTART_STRATEGY_FAILURE_RATE_INTERVAL, 60_000L));
                            final long attemptDelay = properties.getOptionalLong(EXECUTION_RESTART_STRATEGY_DELAY)
                                    .orElseGet(() -> useDefaultValue(EXECUTION_RESTART_STRATEGY_DELAY, 10_000L));
                            return Optional.of(RestartStrategies.failureRateRestart(
                                    failureRate,
                                    Time.milliseconds(failureInterval),
                                    Time.milliseconds(attemptDelay)));
                        default:
                            return Optional.empty();
                    }
                })
                .orElseGet(() ->
                        useDefaultValue(
                                EXECUTION_RESTART_STRATEGY_TYPE,
                                RestartStrategies.fallBackRestart(),
                                EXECUTION_RESTART_STRATEGY_TYPE_VALUE_FALLBACK));
    }

    public Optional<String> getCurrentCatalog() {
        ConfigurationValidater properties = ConfigurationValidater.builder().configuration(configuration).build();
        return properties.getOptionalString(EXECUTION_CURRENT_CATALOG);
    }

    public Optional<String> getCurrentDatabase() {
        ConfigurationValidater properties = ConfigurationValidater.builder().configuration(configuration).build();
        return properties.getOptionalString(EXECUTION_CURRENT_DATABASE);
    }

    public Map<String, String> asTopLevelMap() {
        ConfigurationValidater properties = ConfigurationValidater.builder().configuration(configuration).build();
        return properties.asPrefixedMap(EXECUTION_ENTRY + '.');
    }

    public int getMaxBufferSize() {
        ConfigurationValidater properties = ConfigurationValidater.builder().configuration(configuration).build();
        return properties.getOptionalInt(EXECUTION_MAX_BUFFER_SIZE)
                .orElseGet(() -> useDefaultValue(EXECUTION_MAX_BUFFER_SIZE, 5000));
    }

    private <V> V useDefaultValue(String key, V defaultValue) {
        return useDefaultValue(key, defaultValue, defaultValue.toString());
    }

    private <V> V useDefaultValue(String key, V defaultValue, String defaultString) {
        LOG.info("Property '{}.{}' not specified. Using default value: {}", EXECUTION_ENTRY, key, defaultString);
        return defaultValue;
    }

    // --------------------------------------------------------------------------------------------

    public static ExecutionEntry create(Map<String, Object> config) {
        return new ExecutionEntry(ConfigUtil.normalizeYaml(config));
    }

    /**
     * Merges two execution entries. The properties of the first execution entry might be
     * overwritten by the second one.
     */
    public static ExecutionEntry merge(ExecutionEntry execution1, ExecutionEntry execution2) {
        final Map<String, String> mergedProperties = new HashMap<>(execution1.asMap());
        mergedProperties.putAll(execution2.asMap());
        return new ExecutionEntry(Configuration.fromMap(mergedProperties));
    }

    /**
     * Creates a new execution entry enriched with additional properties that are prefixed with
     * {@link Environment#EXECUTION_ENTRY}.
     */
    public static ExecutionEntry enrich(ExecutionEntry execution, Map<String, String> prefixedProperties) {
        final Map<String, String> enrichedProperties = new HashMap<>(execution.asMap());

        prefixedProperties.forEach((k, v) -> {
            final String normalizedKey = k.toLowerCase();
            if (k.startsWith(EXECUTION_ENTRY + '.')) {
                enrichedProperties.put(normalizedKey.substring(EXECUTION_ENTRY.length() + 1), v);
            }
        });

        return new ExecutionEntry(Configuration.fromMap(enrichedProperties));
    }

}
