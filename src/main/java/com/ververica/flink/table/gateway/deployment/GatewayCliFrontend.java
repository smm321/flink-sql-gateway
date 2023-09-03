package com.ververica.flink.table.gateway.deployment;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.cli.ProgramOptions;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkException;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Implementation of a simple command line frontend for executing programs. */
@Deprecated
public class GatewayCliFrontend extends CliFrontend {

    private final Configuration configuration;

    public GatewayCliFrontend(Configuration configuration, List<CustomCommandLine> customCommandLines) {
        super(configuration, customCommandLines);
        this.configuration = configuration;
    }

    public GatewayCliFrontend(Configuration configuration, ClusterClientServiceLoader clusterClientServiceLoader,
                              List<CustomCommandLine> customCommandLines) {
        super(configuration, clusterClientServiceLoader, customCommandLines);
        this.configuration = configuration;
    }

    public void run(String[] args) throws Exception {
        final String[] params = Arrays.copyOfRange(args, 1, args.length);
        final Options commandOptions = CliFrontendParser.getRunCommandOptions();
        final CommandLine commandLine = getCommandLine(commandOptions, params, true);

        final CustomCommandLine activeCommandLine =
                validateAndGetActiveCommandLine(checkNotNull(commandLine));

        final ProgramOptions programOptions = ProgramOptions.create(commandLine);

        final List<URL> jobJars = getJobJarAndDependencies(programOptions);

        final Configuration effectiveConfiguration =
                getEffectiveConfiguration(activeCommandLine, commandLine, programOptions, jobJars);

        PackagedProgram program = getPackagedProgram(programOptions, effectiveConfiguration);
        executeProgram(effectiveConfiguration, program);
    }

    private List<URL> getJobJarAndDependencies(ProgramOptions programOptions)
            throws CliArgsException {
        String entryPointClass = programOptions.getEntryPointClassName();
        String jarFilePath = programOptions.getJarFilePath();

        try {
            File jarFile = jarFilePath != null ? getJarFile(jarFilePath) : null;
            return PackagedProgram.getJobJarAndDependencies(jarFile, entryPointClass);
        } catch (FileNotFoundException | ProgramInvocationException e) {
            throw new CliArgsException(
                    "Could not get job jar and dependencies from JAR file: " + e.getMessage(), e);
        }
    }

    private File getJarFile(String jarFilePath) throws FileNotFoundException {
        File jarFile = new File(jarFilePath);
        // Check if JAR file exists
        if (!jarFile.exists()) {
            throw new FileNotFoundException("JAR file does not exist: " + jarFile);
        } else if (!jarFile.isFile()) {
            throw new FileNotFoundException("JAR file is not a file: " + jarFile);
        }
        return jarFile;
    }

    private <T> Configuration getEffectiveConfiguration(
            final CustomCommandLine activeCustomCommandLine, final CommandLine commandLine)
            throws FlinkException {

        final Configuration effectiveConfiguration = new Configuration(configuration);

        final Configuration commandLineConfiguration =
                checkNotNull(activeCustomCommandLine).toConfiguration(commandLine);

        effectiveConfiguration.addAll(commandLineConfiguration);

        return effectiveConfiguration;
    }

    private <T> Configuration getEffectiveConfiguration(
            final CustomCommandLine activeCustomCommandLine,
            final CommandLine commandLine,
            final ProgramOptions programOptions,
            final List<T> jobJars)
            throws FlinkException {

        final Configuration effectiveConfiguration =
                getEffectiveConfiguration(activeCustomCommandLine, commandLine);

        final ExecutionConfigAccessor executionParameters =
                ExecutionConfigAccessor.fromProgramOptions(
                        checkNotNull(programOptions), checkNotNull(jobJars));

        executionParameters.applyToConfiguration(effectiveConfiguration);

        return effectiveConfiguration;
    }

    private PackagedProgram getPackagedProgram(
            ProgramOptions programOptions, Configuration effectiveConfiguration)
            throws ProgramInvocationException, CliArgsException {
        PackagedProgram program;
        try {
            program = buildProgram(programOptions, effectiveConfiguration);
        } catch (FileNotFoundException e) {
            throw new CliArgsException(
                    "Could not build the program from JAR file: " + e.getMessage(), e);
        }
        return program;
    }

    PackagedProgram buildProgram(final ProgramOptions runOptions, final Configuration configuration)
            throws FileNotFoundException, ProgramInvocationException, CliArgsException {
        runOptions.validate();

        String[] programArgs = runOptions.getProgramArgs();
        String jarFilePath = runOptions.getJarFilePath();
        List<URL> classpaths = runOptions.getClasspaths();

        // Get assembler class
        String entryPointClass = runOptions.getEntryPointClassName();
        File jarFile = jarFilePath != null ? getJarFile(jarFilePath) : null;

        return PackagedProgram.newBuilder()
                .setJarFile(jarFile)
                .setUserClassPaths(classpaths)
                .setEntryPointClassName(entryPointClass)
                .setConfiguration(configuration)
                .setSavepointRestoreSettings(runOptions.getSavepointRestoreSettings())
                .setArguments(programArgs)
                .build();
    }
}
