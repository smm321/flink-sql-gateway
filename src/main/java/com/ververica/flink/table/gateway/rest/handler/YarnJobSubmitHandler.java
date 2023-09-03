package com.ververica.flink.table.gateway.rest.handler;

import com.ververica.flink.table.gateway.rest.message.YarnJobSubmitRequestBody;
import com.ververica.flink.table.gateway.rest.message.YarnJobSubmitResponseBody;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.cli.CustomCommandLine;
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
import java.io.PrintWriter;
import java.io.StringWriter;
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
            final CliFrontend cliFrontend = new CliFrontend(configuration, customCommandLines);
            SecurityUtils.install(new SecurityConfiguration(cliFrontend.getConfiguration()));
            String[] arrDml = {dml};
            SecurityUtils.getInstalledContext().runSecured(
                    () -> {
                        try {
                            cliFrontend.parseAndRun(Stream.concat(Arrays.stream(cmd.split(" "))
                                            .filter(StringUtils::isNoneEmpty),
                                    Arrays.stream(arrDml)).toArray(String[]::new));
                            return 0;
                        } catch (Exception e) {
                            StringWriter sw = new StringWriter();
                            PrintWriter pw = new PrintWriter(sw);
                            e.printStackTrace(pw);
                            sb.append(sw.toString());
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
}
