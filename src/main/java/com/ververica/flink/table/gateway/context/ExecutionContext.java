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

package com.ververica.flink.table.gateway.context;

import com.ververica.flink.table.gateway.config.Environment;
import com.ververica.flink.table.gateway.config.entries.DeploymentEntry;
import com.ververica.flink.table.gateway.config.entries.TemporalTableEntry;
import com.ververica.flink.table.gateway.config.entries.ViewEntry;
import com.ververica.flink.table.gateway.utils.SqlExecutionException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.cli.ProgramOptions;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.ModuleFactory;
import org.apache.flink.table.factories.PlannerFactoryUtil;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.module.CoreModuleFactory;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.MutableURLClassLoader;
import org.apache.flink.util.TemporaryClassLoaderContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Context for executing table programs. This class caches everything that can be cached across
 * multiple queries as long as the session context does not change. This must be thread-safe as
 * it might be reused across different query submissions.
 *
 * @param <ClusterID> cluster id
 */
public class ExecutionContext<ClusterID> {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutionContext.class);

    private final Environment environment;
    private final ClassLoader classLoader;

    private final Configuration flinkConfig;
    private final ClusterClientFactory<ClusterID> clusterClientFactory;

    private ExecutionEnvironment execEnv;
    private StreamExecutionEnvironment streamExecEnv;
    private TableEnvironment tableEnv;
    private Executor executor;
    private FunctionCatalog functionCatalog;

    // Members that should be reused in the same session.
    private SessionState sessionState;

    private ExecutionContext(
            Environment environment,
            @Nullable SessionState sessionState,
            List<URL> dependencies,
            Configuration flinkConfig,
            ClusterClientServiceLoader clusterClientServiceLoader,
            Options commandLineOptions,
            List<CustomCommandLine> availableCommandLines) throws FlinkException {
        this.environment = environment;

        this.flinkConfig = flinkConfig;

        // create class loader
        classLoader = ClientUtils.buildUserCodeClassLoader(
                dependencies,
                Collections.emptyList(),
                this.getClass().getClassLoader(),
                flinkConfig);

        // Initialize the TableEnvironment.
        initializeTableEnvironment(sessionState);

        LOG.debug("Deployment descriptor: {}", environment.getDeployment());
        final CommandLine commandLine = createCommandLine(
                environment.getDeployment(),
                commandLineOptions);

        flinkConfig.addAll(createExecutionConfig(
                commandLine,
                commandLineOptions,
                availableCommandLines,
                dependencies));

        final ClusterClientServiceLoader serviceLoader = checkNotNull(clusterClientServiceLoader);
        clusterClientFactory = serviceLoader.getClusterClientFactory(flinkConfig);
        checkState(clusterClientFactory != null);
    }

    public StreamTableEnvironmentImpl getStreamTableEnvironmentImpl() {
        return (StreamTableEnvironmentImpl) tableEnv;
    }

    public Configuration getFlinkConfig() {
        return flinkConfig;
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public Environment getEnvironment() {
        return environment;
    }

    public ExecutionEnvironment getExecEnv(){
        return this.execEnv;
    }

    public ClusterDescriptor<ClusterID> createClusterDescriptor(Configuration configuration) {
        return clusterClientFactory.createClusterDescriptor(configuration);
    }

    public Map<String, Catalog> getCatalogs() {
        Map<String, Catalog> catalogs = new HashMap<>();
        for (String name : tableEnv.listCatalogs()) {
            tableEnv.getCatalog(name).ifPresent(c -> catalogs.put(name, c));
        }
        return catalogs;
    }

    public SessionState getSessionState() {
        return this.sessionState;
    }

    /**
     * Executes the given supplier using the execution context's classloader as thread classloader.
     */
    public <R> R wrapClassLoader(Supplier<R> supplier) {
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
            return supplier.get();
        }
    }

    /**
     * Executes the given Runnable using the execution context's classloader as thread classloader.
     */
    void wrapClassLoader(Runnable runnable) {
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
            runnable.run();
        }
    }

    public TableEnvironment getTableEnvironment() {
        return tableEnv;
    }

    public ExecutionConfig getExecutionConfig() {
        if (streamExecEnv != null) {
            return streamExecEnv.getConfig();
        } else {
            return execEnv.getConfig();
        }
    }

    public ClusterClientFactory<ClusterID> getClusterClientFactory() {
        return clusterClientFactory;
    }

    public Pipeline createPipeline(String name, List<Transformation<?>> operations) {
        return wrapClassLoader(() -> {
            return executor.createPipeline(operations, tableEnv.getConfig(), name);
        });
    }

    /**
     * Returns a builder for this {@link ExecutionContext}.
     */
    public static Builder builder(
            Environment defaultEnv,
            Environment sessionEnv,
            List<URL> dependencies,
            Configuration configuration,
            ClusterClientServiceLoader serviceLoader,
            Options commandLineOptions,
            List<CustomCommandLine> commandLines) {
        return new Builder(defaultEnv, sessionEnv, dependencies, configuration,
                serviceLoader, commandLineOptions, commandLines);
    }

    //------------------------------------------------------------------------------------------------------------------
    // Non-public methods
    //------------------------------------------------------------------------------------------------------------------

    private static Configuration createExecutionConfig(
            CommandLine commandLine,
            Options commandLineOptions,
            List<CustomCommandLine> availableCommandLines,
            List<URL> dependencies) throws FlinkException {
        LOG.debug("Available commandline options: {}", commandLineOptions);
        List<String> options = Stream
                .of(commandLine.getOptions())
                .map(o -> o.getOpt() + "=" + o.getValue())
                .collect(Collectors.toList());
        LOG.debug(
                "Instantiated commandline args: {}, options: {}",
                commandLine.getArgList(),
                options);

        final CustomCommandLine activeCommandLine = findActiveCommandLine(
                availableCommandLines,
                commandLine);
        LOG.debug(
                "Available commandlines: {}, active commandline: {}",
                availableCommandLines,
                activeCommandLine);

        Configuration executionConfig = activeCommandLine.toConfiguration(commandLine);

        try {
            final ProgramOptions programOptions = ProgramOptions.create(commandLine);
            final ExecutionConfigAccessor executionConfigAccessor = ExecutionConfigAccessor
                    .fromProgramOptions(programOptions, dependencies);
            executionConfigAccessor.applyToConfiguration(executionConfig);
        } catch (CliArgsException e) {
            throw new SqlExecutionException("Invalid deployment run options.", e);
        }

        LOG.info("Executor config: {}", executionConfig);
        return executionConfig;
    }

    private static CommandLine createCommandLine(DeploymentEntry deployment, Options commandLineOptions) {
        try {
            return deployment.getCommandLine(commandLineOptions);
        } catch (Exception e) {
            throw new SqlExecutionException("Invalid deployment options.", e);
        }
    }

    private static CustomCommandLine findActiveCommandLine(List<CustomCommandLine> availableCommandLines,
                                                           CommandLine commandLine) {
        for (CustomCommandLine cli : availableCommandLines) {
            if (cli.isActive(commandLine)) {
                return cli;
            }
        }
        throw new SqlExecutionException("Could not find a matching deployment.");
    }

    private Module createModule(Map<String, String> moduleProperties, ClassLoader classLoader) {
        final ModuleFactory factory =
                TableFactoryService.find(ModuleFactory.class, moduleProperties, classLoader);
        return factory.createModule(moduleProperties);
    }

    private Catalog createCatalog(String name, Map<String, String> catalogProperties, ClassLoader classLoader) {

        Catalog catalog =
                FactoryUtil.createCatalog(
                        name,
                        catalogProperties,
                        tableEnv.getConfig().getConfiguration(),
                        classLoader);
        return catalog;
//		final CatalogFactory factory =
//			TableFactoryService.find(CatalogFactory.class, catalogProperties, classLoader);
//		return factory.createCatalog(name, catalogProperties);
    }

//    private static TableSource<?> createTableSource(ExecutionEntry execution, Map<String, String> sourceProperties,
//                                                    ClassLoader classLoader) {
//        if (execution.isStreamingPlanner()) {
//            final TableSourceFactory<?> factory = (TableSourceFactory<?>)
//                    TableFactoryService.find(TableSourceFactory.class, sourceProperties, classLoader);
//            return factory.createTableSource(sourceProperties);
//        } else if (execution.isBatchPlanner()) {
//            final BatchTableSourceFactory<?> factory = (BatchTableSourceFactory<?>)
//                    TableFactoryService.find(BatchTableSourceFactory.class, sourceProperties, classLoader);
//            return factory.createBatchTableSource(sourceProperties);
//        }
//        throw new SqlExecutionException("Unsupported execution type for sources.");
//    }

//    private static TableSink<?> createTableSink(ExecutionEntry execution, Map<String, String> sinkProperties,
//                                                ClassLoader classLoader) {
//        if (execution.isStreamingPlanner()) {
//            final TableSinkFactory<?> factory = (TableSinkFactory<?>)
//                    TableFactoryService.find(TableSinkFactory.class, sinkProperties, classLoader);
//            return factory.createTableSink(sinkProperties);
//        } else if (execution.isBatchPlanner()) {
//            final BatchTableSinkFactory<?> factory = (BatchTableSinkFactory<?>)
//                    TableFactoryService.find(BatchTableSinkFactory.class, sinkProperties, classLoader);
//            return factory.createBatchTableSink(sinkProperties);
//        }
//        throw new SqlExecutionException("Unsupported execution type for sinks.");
//    }

    private TableEnvironment createStreamTableEnvironment(
            StreamExecutionEnvironment env,
            EnvironmentSettings settings,
            TableConfig config,
            Executor executor,
            CatalogManager catalogManager,
            ModuleManager moduleManager,
            FunctionCatalog functionCatalog) {

        final MutableURLClassLoader userClassLoader =
                FlinkUserCodeClassLoaders.create(
                        new URL[0], settings.getUserClassLoader(), settings.getConfiguration());
        final Planner planner = PlannerFactoryUtil.createPlanner(executor,
                config, userClassLoader, moduleManager, catalogManager, functionCatalog);

        final ResourceManager resourceManager =
                new ResourceManager(settings.getConfiguration(), userClassLoader);

        return new StreamTableEnvironmentImpl(
                catalogManager,
                moduleManager,
                resourceManager,
                functionCatalog,
                config,
                env,
                planner,
                executor,
                settings.isStreamingMode()
                );
    }

    private static Executor lookupExecutor(
            StreamExecutionEnvironment executionEnvironment, ClassLoader classLoader) {
        try {
            final ExecutorFactory executorFactory =
                    FactoryUtil.discoverFactory(
                            classLoader,
                            ExecutorFactory.class,
                            ExecutorFactory.DEFAULT_IDENTIFIER);
            final Method createMethod =
                    executorFactory
                            .getClass()
                            .getMethod("create", StreamExecutionEnvironment.class);

            return (Executor) createMethod.invoke(executorFactory, executionEnvironment);
        } catch (Exception e) {
            throw new TableException(
                    "Could not instantiate the executor. Make sure a planner module is on the classpath",
                    e);
        }
    }

    private void initializeTableEnvironment(@Nullable SessionState sessionState) {
        final EnvironmentSettings settings = environment.getExecution().getEnvironmentSettings();
        // Step 0.0 Initialize the table configuration.
        final TableConfig config = TableConfig.getDefault();
        environment.getConfiguration().asMap().forEach((k, v) ->
                config.getConfiguration().setString(k, v));
        final boolean noInheritedState = sessionState == null;
        if (noInheritedState) {
            //--------------------------------------------------------------------------------------------------------------
            // Step.1 Create environments
            //--------------------------------------------------------------------------------------------------------------
            // Step 1.0 Initialize the ModuleManager if required.
            final ModuleManager moduleManager = new ModuleManager();
            // Step 1.1 Initialize the CatalogManager if required.
            final CatalogManager catalogManager = CatalogManager.newBuilder()
                    .classLoader(classLoader)
                    .config(config.getConfiguration())
                    .defaultCatalog(
                            settings.getBuiltInCatalogName(),
                            new GenericInMemoryCatalog(
                                    settings.getBuiltInCatalogName(),
                                    settings.getBuiltInDatabaseName()))
                    .build();

            // Step 1.2 Initialize the FunctionCatalog if required.
            final MutableURLClassLoader mutableURLClassLoader =
                    FlinkUserCodeClassLoaders.create(
                            new URL[0], settings.getUserClassLoader(), settings.getConfiguration());
            final ResourceManager resourceManager =
                    new ResourceManager(settings.getConfiguration(), mutableURLClassLoader);
            functionCatalog = new FunctionCatalog(config, resourceManager, catalogManager, moduleManager);
            // Step 1.3 Set up session state.
            this.sessionState = SessionState.of(catalogManager, moduleManager, functionCatalog);

            // Must initialize the table environment before actually the
            createTableEnvironment(settings, config, catalogManager, moduleManager, functionCatalog);

            //--------------------------------------------------------------------------------------------------------------
            // Step.2 Create modules and load them into the TableEnvironment.
            //--------------------------------------------------------------------------------------------------------------
            // No need to register the modules info if already inherit from the same session.
            Map<String, Module> modules = new LinkedHashMap<>();
            environment.getModules().forEach((name, entry) ->
                    modules.put(name, createModule(entry.asMap(), classLoader))
            );
            if (!modules.isEmpty()) {
                // unload core module first to respect whatever users configure
                tableEnv.unloadModule(CoreModuleFactory.IDENTIFIER);
                modules.forEach(tableEnv::loadModule);
            }

            //--------------------------------------------------------------------------------------------------------------
            // Step.3 Create catalogs and register them.
            //--------------------------------------------------------------------------------------------------------------
            // No need to register the catalogs if already inherit from the same session.
            initializeCatalogs();

            //--------------------------------------------------------------------------------------------------------------
            // Step.4 create user-defined functions and temporal tables then register them.
            //--------------------------------------------------------------------------------------------------------------
            // No need to register the functions if already inherit from the same session.
            registerFunctions();
            //--------------------------------------------------------------------------------------------------------------
            // Step.5 Enable dynamic-table-options.
            //--------------------------------------------------------------------------------------------------------------
            // todo !!
            tableEnv.getConfig().getConfiguration().setBoolean("table.dynamic-table-options.enabled", true);
        } else {
            // Set up session state.
            this.sessionState = sessionState;
            createTableEnvironment(
                    settings,
                    config,
                    sessionState.catalogManager,
                    sessionState.moduleManager,
                    sessionState.functionCatalog);
        }
    }

    private void createTableEnvironment(
            EnvironmentSettings settings,
            TableConfig config,
            CatalogManager catalogManager,
            ModuleManager moduleManager,
            FunctionCatalog functionCatalog) {
        if (environment.getExecution().isStreamingPlanner()) {
            streamExecEnv = createStreamExecutionEnvironment();
            execEnv = null;

            executor = lookupExecutor(streamExecEnv, classLoader);
            tableEnv = createStreamTableEnvironment(
                    streamExecEnv,
                    settings,
                    config,
                    executor,
                    catalogManager,
                    moduleManager,
                    functionCatalog);
        } else {
            throw new SqlExecutionException("Unsupported execution type specified.");
        }
    }

    private void initializeCatalogs() {
        //--------------------------------------------------------------------------------------------------------------
        // Step.1 Create catalogs and register them.
        //--------------------------------------------------------------------------------------------------------------
        wrapClassLoader(() -> {
            environment.getCatalogs().forEach((name, entry) -> {
                Catalog catalog = createCatalog(name, entry.asMap(), classLoader);
                tableEnv.registerCatalog(name, catalog);
                tableEnv.useCatalog(name);
            });
        });

        // As all of the tables are stored in hive catalog, we don't need to this so far.
        //--------------------------------------------------------------------------------------------------------------
        // Step.2 create table sources & sinks, and register them.
        //--------------------------------------------------------------------------------------------------------------
//        Map<String, TableSource<?>> tableSources = new HashMap<>();
//        Map<String, TableSink<?>> tableSinks = new HashMap<>();
//        environment.getTables().forEach((name, entry) -> {
//			if (entry instanceof SourceTableEntry || entry instanceof SourceSinkTableEntry) {
//				tableSources.put(name, createTableSource(environment.getExecution(), entry.asMap(), classLoader));
//			}
//			if (entry instanceof SinkTableEntry || entry instanceof SourceSinkTableEntry) {
//				tableSinks.put(name, createTableSink(environment.getExecution(), entry.asMap(), classLoader));
//			}
//            tableEnv.createTemporaryView(name, tableEnv.scan(name));
//        });

        // register table sources
//		tableSources.forEach((k, v) -> {
//			tableEnv.createTemporaryView(k, tableEnv.fromTableSource(v));
//		});
        // register table sinks
        // tableSinks.forEach(tableEnv::registerTableSink);

        //--------------------------------------------------------------------------------------------------------------
        // Step.4 Register temporal tables.
        //--------------------------------------------------------------------------------------------------------------
        environment.getTables().forEach((name, entry) -> {
            if (entry instanceof TemporalTableEntry) {
                final TemporalTableEntry temporalTableEntry = (TemporalTableEntry) entry;
                registerTemporalTable(temporalTableEntry);
            }
        });

        //--------------------------------------------------------------------------------------------------------------
        // Step.5 Register views in specified order.
        //--------------------------------------------------------------------------------------------------------------
        environment.getTables().forEach((name, entry) -> {
            // if registering a view fails at this point,
            // it means that it accesses tables that are not available anymore
            if (entry instanceof ViewEntry) {
                final ViewEntry viewEntry = (ViewEntry) entry;
                registerView(viewEntry);
            }
        });

        //--------------------------------------------------------------------------------------------------------------
        // Step.6 Set current catalog and database.
        //--------------------------------------------------------------------------------------------------------------
        // Switch to the current catalog.
        Optional<String> catalog = environment.getExecution().getCurrentCatalog();
        catalog.ifPresent(tableEnv::useCatalog);

        // Switch to the current database.
        Optional<String> database = environment.getExecution().getCurrentDatabase();
        database.ifPresent(tableEnv::useDatabase);
    }

    private ExecutionEnvironment createExecutionEnvironment() {
        final ExecutionEnvironment execEnv = ExecutionEnvironment.getExecutionEnvironment();
        execEnv.setRestartStrategy(environment.getExecution().getRestartStrategy());
        execEnv.setParallelism(environment.getExecution().getParallelism());
        return execEnv;
    }

    private StreamExecutionEnvironment createStreamExecutionEnvironment() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(environment.getExecution().getRestartStrategy());
        env.setParallelism(environment.getExecution().getParallelism());
        env.setMaxParallelism(environment.getExecution().getMaxParallelism());
        env.setStreamTimeCharacteristic(environment.getExecution().getTimeCharacteristic());
        if (env.getStreamTimeCharacteristic() == TimeCharacteristic.EventTime) {
            env.getConfig().setAutoWatermarkInterval(environment.getExecution().getPeriodicWatermarksInterval());
        }
        if (environment.getExecution().getCheckpointInterval() > 0) {
            env.enableCheckpointing(environment.getExecution().getCheckpointInterval());
        }
        return env;
    }

    private void registerFunctions() {
        environment.getFunctions().values().forEach(o -> tableEnv.executeSql(o.getFunctionsDdl()));
    }

    private void registerView(ViewEntry viewEntry) {
        try {
            tableEnv.registerTable(viewEntry.getName(), tableEnv.sqlQuery(viewEntry.getQuery()));
        } catch (Exception e) {
            throw new SqlExecutionException(
                    "Invalid view '" + viewEntry.getName() + "' with query:\n" + viewEntry.getQuery()
                            + "\nCause: " + e.getMessage());
        }
    }

    private void registerTemporalTable(TemporalTableEntry temporalTableEntry) {
        try {
            final Table table = tableEnv.from(temporalTableEntry.getHistoryTable());
            final TableFunction<?> function = table.createTemporalTableFunction(
                    Expressions.$(temporalTableEntry.getTimeAttribute()),
                    Expressions.$(String.join(",", temporalTableEntry.getPrimaryKeyFields())));
            StreamTableEnvironment streamTableEnvironment = (StreamTableEnvironment) tableEnv;
            streamTableEnvironment.createTemporarySystemFunction(temporalTableEntry.getName(), function);
        } catch (Exception e) {
            throw new SqlExecutionException(
                    "Invalid temporal table '" + temporalTableEntry.getName() + "' over table '" +
                            temporalTableEntry.getHistoryTable() + ".\nCause: " + e.getMessage());
        }
    }

    public Executor getExecutor() {
        return executor;
    }

    public FunctionCatalog getFunctionCatalog() {
        return functionCatalog;
    }

    //~ Inner Class -------------------------------------------------------------------------------

    /**
     * Builder for {@link ExecutionContext}.
     */
    public static class Builder {
        // Required members.
        private final Environment sessionEnv;
        private final List<URL> dependencies;
        private final Configuration configuration;
        private final ClusterClientServiceLoader serviceLoader;
        private final Options commandLineOptions;
        private final List<CustomCommandLine> commandLines;

        private Environment defaultEnv;
        private Environment currentEnv;

        // Optional members.
        @Nullable
        private SessionState sessionState;

        private Builder(
                Environment defaultEnv,
                @Nullable Environment sessionEnv,
                List<URL> dependencies,
                Configuration configuration,
                ClusterClientServiceLoader serviceLoader,
                Options commandLineOptions,
                List<CustomCommandLine> commandLines) {
            this.defaultEnv = defaultEnv;
            this.sessionEnv = sessionEnv;
            this.dependencies = dependencies;
            this.configuration = configuration;
            this.serviceLoader = serviceLoader;
            this.commandLineOptions = commandLineOptions;
            this.commandLines = commandLines;
        }

        public Builder env(Environment environment) {
            this.currentEnv = environment;
            return this;
        }

        public Builder sessionState(SessionState sessionState) {
            this.sessionState = sessionState;
            return this;
        }

        public ExecutionContext<?> build() {
            try {
                return new ExecutionContext<>(
                        this.currentEnv == null ? Environment.merge(defaultEnv, sessionEnv) : this.currentEnv,
                        this.sessionState,
                        this.dependencies,
                        this.configuration,
                        this.serviceLoader,
                        this.commandLineOptions,
                        this.commandLines);
            } catch (Throwable t) {
                // catch everytt = {CatalogException@6135} "org.apache.flink.table.catalog.exceptions.CatalogException: Failed to create Hive Metastore client"hing such that a configuration does not crash the executor
                throw new SqlExecutionException("Could not create execution context.", t);
            }
        }
    }

    /**
     * Represents the state that should be reused in one session.
     **/
    public static class SessionState {
        public final CatalogManager catalogManager;
        public final ModuleManager moduleManager;
        public final FunctionCatalog functionCatalog;

        private SessionState(
                CatalogManager catalogManager,
                ModuleManager moduleManager,
                FunctionCatalog functionCatalog) {
            this.catalogManager = catalogManager;
            this.moduleManager = moduleManager;
            this.functionCatalog = functionCatalog;
        }

        public static SessionState of(
                CatalogManager catalogManager,
                ModuleManager moduleManager,
                FunctionCatalog functionCatalog) {
            return new SessionState(catalogManager, moduleManager, functionCatalog);
        }
    }
}
