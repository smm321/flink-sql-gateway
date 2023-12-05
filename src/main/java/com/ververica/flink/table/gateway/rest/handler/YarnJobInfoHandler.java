package com.ververica.flink.table.gateway.rest.handler;

import com.ververica.flink.table.gateway.rest.entity.YarnJobInfoResult;
import com.ververica.flink.table.gateway.rest.message.YarnJobInfoResponseBody;
import com.ververica.flink.table.gateway.utils.FlinkUtil;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Handler for getting a job's info.
 */
public class YarnJobInfoHandler extends YarnJobAbstractHandler<EmptyRequestBody, YarnJobInfoResponseBody> {
    public YarnJobInfoHandler(Time timeout, Map responseHeaders,
                              MessageHeaders<EmptyRequestBody,
                                      YarnJobInfoResponseBody, EmptyMessageParameters> messageHeaders) {
        super(timeout, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture handleRequest(@Nonnull HandlerRequest<EmptyRequestBody> request)
            throws RestHandlerException {
        List<YarnJobInfoResult> infos = new ArrayList<>();
        try {
            List<ApplicationReport> reports = yarnClient.getApplications(Collections.singleton("Apache Flink"),
                    EnumSet.of(YarnApplicationState.RUNNING));

            reports.forEach(report -> {
                YarnJobInfoResult result = new YarnJobInfoResult();
                result.setAppId(report.getApplicationId().toString());
                result.setJobName(report.getName());
                result.setTrackingUrl(report.getTrackingUrl());
                result.setFlinkJob(FlinkUtil.getJobInofs(report.getTrackingUrl() + "/jobs/overview"));
                result.setYarnState(report.getYarnApplicationState().toString());
                result.setYarnStartTime(report.getStartTime());
                infos.add(result);
            });
        } catch (Exception e) {
            throw new RestHandlerException(e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
        }
        return CompletableFuture.completedFuture(new YarnJobInfoResponseBody("", infos));
    }
}
