package com.ververica.flink.table.gateway.rest.handler;

import com.ververica.flink.table.gateway.deployment.GatewayCliFrontend;
import com.ververica.flink.table.gateway.rest.message.YarnJobSubmitRequestBody;
import com.ververica.flink.table.gateway.rest.message.YarnJobSubmitResponseBody;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nonnull;
import java.io.File;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.apache.flink.client.cli.CliFrontend.loadCustomCommandLines;

/**
 * Handler for submitting a Flink job.
 */
@Slf4j
public class YarnJobSubmitHandler extends YarnJobAbstractHandler<YarnJobSubmitRequestBody, YarnJobSubmitResponseBody> {

    public YarnJobSubmitHandler(Time timeout, Map responseHeaders,
                                MessageHeaders<YarnJobSubmitRequestBody,
                                        YarnJobSubmitResponseBody, EmptyMessageParameters> messageHeaders) {
        super(timeout, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture handleRequest(@Nonnull HandlerRequest<YarnJobSubmitRequestBody> request)
            throws RestHandlerException {
        String cmd = request.getRequestBody().getCmd();
        String dml = request.getRequestBody().getDml();
        log.info(String.format("YarnJobSubmitHandler receive request cmd:%s dml%s", cmd, dml));
        final String configurationDirectory = getConfigurationDirectoryFromEnv();

        final Configuration configuration =
                GlobalConfiguration.loadConfiguration(configurationDirectory);

        final List<CustomCommandLine> customCommandLines =
                loadCustomCommandLines(configuration, configurationDirectory);

        // todo
        // we should return yarn application id when deploy success
        StringBuffer sb = new StringBuffer();
        try {
            final GatewayCliFrontend cli = new GatewayCliFrontend(configuration, customCommandLines);
            SecurityUtils.install(new SecurityConfiguration(cli.getConfiguration()));
            String[] arrDml = {dml};
            SecurityUtils.getInstalledContext().runSecured(
                    () -> {
                        try {
                            cli.run(Stream.concat(Arrays.stream(cmd.split(" "))
                                            .filter(o -> StringUtils.isNoneEmpty(o)),
                                    Arrays.stream(arrDml)).toArray(String[]::new));
                            return 0;
                        } catch (Exception e) {
                            sb.append(e.getMessage()).append(System.lineSeparator());
                        }
                        return 1;
                    }
            );
        } catch (Throwable t) {
            final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
            strippedThrowable.printStackTrace();
            sb.append(strippedThrowable.getMessage()).append(System.lineSeparator());
            log.error("YarnJobSubmitHandler deploy job error.", t);
        }

        return CompletableFuture.completedFuture(new YarnJobSubmitResponseBody(sb.toString(), ""));
    }

    private String getConfigurationDirectoryFromEnv() {
        String location = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);

        if (location != null) {
            if (new File(location).exists()) {
                return location;
            } else {
                throw new RuntimeException(
                        "The configuration directory '"
                                + location
                                + "', specified in the '"
                                + ConfigConstants.ENV_FLINK_CONF_DIR
                                + "' environment variable, does not exist.");
            }
        } else if (new File("../conf").exists()) {
            location = "../conf";
        } else if (new File("../conf").exists()) {
            location = "conf";
        } else {
            throw new RuntimeException(
                    "The configuration directory was not specified. "
                            + "Please specify the directory containing the configuration file through the '"
                            + ConfigConstants.ENV_FLINK_CONF_DIR
                            + "' environment variable.");
        }
        return location;
    }
}
