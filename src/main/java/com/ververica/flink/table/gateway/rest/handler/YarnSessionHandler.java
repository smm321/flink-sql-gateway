package com.ververica.flink.table.gateway.rest.handler;

import com.ververica.flink.table.gateway.rest.message.YarnJobSubmitRequestBody;
import com.ververica.flink.table.gateway.rest.message.YarnJobSubmitResponseBody;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.DynamicPropertiesUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;

import javax.annotation.Nonnull;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.client.cli.CliFrontendParser.DETACHED_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.YARN_DETACHED_OPTION;

/**
 * Handler for lunch a flink session.
 */
@Slf4j
public class YarnSessionHandler extends YarnJobAbstractHandler<YarnJobSubmitRequestBody, YarnJobSubmitResponseBody> {

    public YarnSessionHandler(Time timeout, Map responseHeaders,
                              MessageHeaders<YarnJobSubmitRequestBody,
                                        YarnJobSubmitResponseBody, EmptyMessageParameters> messageHeaders) {
        super(timeout, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture handleRequest(@Nonnull HandlerRequest<YarnJobSubmitRequestBody> request)
            throws RestHandlerException {
        String cmd = request.getRequestBody().getCmd();
        String dml = request.getRequestBody().getDml();
        log.info(String.format("YarnSessionHandler receive request cmd:%s dml%s", cmd, dml));
        final String configurationDirectory = getConfigurationDirectoryFromEnv();

        final Configuration configuration =
                GlobalConfiguration.loadConfiguration(configurationDirectory);

        configuration.set(DeploymentOptions.TARGET, YarnDeploymentTarget.SESSION.getName());

        // todo
        // we should return yarn application id when deploy success
        StringBuffer sb = new StringBuffer();
        int retCode;
        try {
            final FlinkYarnSessionCli cli =
                    new FlinkYarnSessionCli(
                            configuration,
                            configurationDirectory,
                            "",
                            ""); // no prefix for the YARN session

            final CommandLine commandLine = CliFrontendParser.parse(initAllOptions(), cmd.split(" "), true);
            final Configuration securityFlinkConfiguration = configuration.clone();
            DynamicPropertiesUtil.encodeDynamicProperties(commandLine, securityFlinkConfiguration);
            SecurityUtils.install(new SecurityConfiguration(securityFlinkConfiguration));
            retCode = SecurityUtils.getInstalledContext().runSecured(() -> cli.run(cmd.split(" ")));

            if (retCode != 0) {
                throw new RuntimeException("submit yarn-session error");
            }
        } catch (Throwable t) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            t.printStackTrace(pw);
            sb.append(sw.toString());
            log.error("YarnSessionHandler deploy job error.", t);
        }

        return CompletableFuture.completedFuture(new YarnJobSubmitResponseBody(sb.toString(), ""));
    }

    private static Options initAllOptions() {
        String shortPrefix = "";
        String longPrefix = "";
        Option query =
                new Option(
                        shortPrefix + "q",
                        longPrefix + "query",
                        false,
                        "Display available YARN resources (memory, cores)");
        Option queue = new Option(shortPrefix + "qu", longPrefix + "queue", true, "Specify YARN queue.");
        Option shipPath =
                new Option(
                        shortPrefix + "t",
                        longPrefix + "ship",
                        true,
                        "Ship files in the specified directory (t for transfer)");
        Option flinkJar =
                new Option(shortPrefix + "j", longPrefix + "jar", true, "Path to Flink jar file");
        Option jmMemory =
                new Option(
                        shortPrefix + "jm",
                        longPrefix + "jobManagerMemory",
                        true,
                        "Memory for JobManager Container with optional unit (default: MB)");
        Option tmMemory =
                new Option(
                        shortPrefix + "tm",
                        longPrefix + "taskManagerMemory",
                        true,
                        "Memory per TaskManager Container with optional unit (default: MB)");
        Option slots =
                new Option(
                        shortPrefix + "s",
                        longPrefix + "slots",
                        true,
                        "Number of slots per TaskManager");
        Option dynamicproperties =
                Option.builder(shortPrefix + "D")
                        .argName("property=value")
                        .numberOfArgs(2)
                        .valueSeparator()
                        .desc("use value for given property")
                        .build();
        Option name =
                new Option(
                        shortPrefix + "nm",
                        longPrefix + "name",
                        true,
                        "Set a custom name for the application on YARN");
        Option applicationType =
                new Option(
                        shortPrefix + "at",
                        longPrefix + "applicationType",
                        true,
                        "Set a custom application type for the application on YARN");
        Option zookeeperNamespace =
                new Option(
                        shortPrefix + "z",
                        longPrefix + "zookeeperNamespace",
                        true,
                        "Namespace to create the Zookeeper sub-paths for high availability mode");
        Option nodeLabel =
                new Option(
                        shortPrefix + "nl",
                        longPrefix + "nodeLabel",
                        true,
                        "Specify YARN node label for the YARN application");

        Options allOptions = new Options();
        allOptions.addOption(flinkJar);
        allOptions.addOption(jmMemory);
        allOptions.addOption(tmMemory);
        allOptions.addOption(queue);
        allOptions.addOption(query);
        allOptions.addOption(shipPath);
        allOptions.addOption(slots);
        allOptions.addOption(dynamicproperties);
        allOptions.addOption(DETACHED_OPTION);
        allOptions.addOption(YARN_DETACHED_OPTION);
        allOptions.addOption(name);
        allOptions.addOption(applicationType);
        allOptions.addOption(zookeeperNamespace);
        allOptions.addOption(nodeLabel);

        return allOptions;
    }
}
