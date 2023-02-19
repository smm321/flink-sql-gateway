package com.ververica.flink.table.gateway.rest.handler;

import com.ververica.flink.table.gateway.rest.message.YarnJobSubmitRequestBody;
import com.ververica.flink.table.gateway.rest.message.YarnJobSubmitResponseBody;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.cli.CliFrontend;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.client.cli.CliFrontend.loadCustomCommandLines;

/**
 * Handler for submitting a Flink job.
 */
public class YarnJobSubmitHandler extends YarnJobAbstractHandler<YarnJobSubmitRequestBody, YarnJobSubmitResponseBody> {
    public YarnJobSubmitHandler(Time timeout, Map responseHeaders,
                                MessageHeaders<YarnJobSubmitRequestBody,
                                        YarnJobSubmitResponseBody, EmptyMessageParameters> messageHeaders) {
        super(timeout, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture handleRequest(@Nonnull HandlerRequest<YarnJobSubmitRequestBody,
            EmptyMessageParameters> request) throws RestHandlerException {
        String cmd = request.getRequestBody().getCmd();

        final String configurationDirectory = getConfigurationDirectoryFromEnv();

        final Configuration configuration =
                GlobalConfiguration.loadConfiguration(configurationDirectory);

        final List<CustomCommandLine> customCommandLines =
                loadCustomCommandLines(configuration, configurationDirectory);

        // todo
        // we should return yarn application id when deploy success
        String errMsg = "";
        try {
            final CliFrontend cli = new CliFrontend(configuration, customCommandLines);
            SecurityUtils.install(new SecurityConfiguration(cli.getConfiguration()));
            int retCode = SecurityUtils.getInstalledContext().runSecured(() -> cli.parseAndRun(cmd.split(" ")));
            if (0 != retCode) {
                //unknown error
                throw new RuntimeException("job start failed");
            }
        } catch (Throwable t) {
            final Throwable strippedThrowable =
                    ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
            t.getCause().printStackTrace();
            strippedThrowable.printStackTrace();
            errMsg = t.getMessage();
        }

        return CompletableFuture.completedFuture(new YarnJobSubmitResponseBody(errMsg, ""));
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
