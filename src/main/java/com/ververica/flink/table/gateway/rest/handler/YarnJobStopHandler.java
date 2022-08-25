package com.ververica.flink.table.gateway.rest.handler;

import com.ververica.flink.table.gateway.rest.entity.YarnJobStopResult;
import com.ververica.flink.table.gateway.rest.message.YarnJobStopRequestBody;
import com.ververica.flink.table.gateway.rest.message.YarnJobStopResponseBody;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterClientFactory;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Handler for canceling a Flink job with triggering savepoint gracefully.
 */
public class YarnJobStopHandler extends YarnJobAbstractHandler<YarnJobStopRequestBody, YarnJobStopResponseBody> {
    public YarnJobStopHandler(Time timeout, Map responseHeaders, MessageHeaders<YarnJobStopRequestBody,
            YarnJobStopResponseBody, EmptyMessageParameters> messageHeaders) {
        super(timeout, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture handleRequest(@Nonnull HandlerRequest<YarnJobStopRequestBody,
            EmptyMessageParameters> request) throws RestHandlerException {
        String name = request.getRequestBody().getJobName();
        List<YarnJobStopResult> list = new ArrayList<>();
        try {
            List<ApplicationReport> reports = yarnClient.getApplications(Collections.singleton("Apache Flink"),
                    EnumSet.of(YarnApplicationState.RUNNING));
            List<ApplicationId> appIds = reports.stream()
                    .filter(o -> o.getName().equals(name))
                    .map(o -> o.getApplicationId())
                    .collect(Collectors.toList());
            if (CollectionUtils.isEmpty(appIds)) {
                return CompletableFuture.completedFuture(new YarnJobStopResponseBody("NOT JOB FOUND"));
            }

            for (int i = 0; i < appIds.size(); i++) {
                ApplicationId appid = appIds.get(i);
                Configuration configuration = new Configuration();
                String appId = appid.toString();
                configuration.set(YarnConfigOptions.APPLICATION_ID, appId);
                YarnClusterClientFactory clusterClientFactory = new YarnClusterClientFactory();
                ApplicationId applicationId = clusterClientFactory.getClusterId(configuration);
                YarnClusterDescriptor clusterDescriptor = new YarnClusterDescriptor(configuration,
                        yarnConfiguration,
                        yarnClient,
                        YarnClientYarnClusterInformationRetriever.create(yarnClient),
                        true);

                try (ClusterClient<ApplicationId> clusterClient = clusterDescriptor.retrieve(applicationId).getClusterClient()) {
                    Collection<JobStatusMessage> jobList = clusterClient.listJobs().get();
                    Iterator it = jobList.stream().iterator();
                    while (it.hasNext()) {
                        JobStatusMessage message = (JobStatusMessage) it.next();
                        JobID jobId = message.getJobId();
                        String jobName = message.getJobName();
                        String location = clusterClient.stopWithSavepoint(jobId,
                                true,
                                "hdfs:///checkpoint/" + jobName + "/" + jobId.toHexString()).get();
                        YarnJobStopResult yarnJobStopResult = new YarnJobStopResult();
                        yarnJobStopResult.setAppId(appId);
                        yarnJobStopResult.setJobId(jobId.toHexString());
                        yarnJobStopResult.setJobName(jobName);
                        yarnJobStopResult.setSavepointUrl(location);
                        list.add(yarnJobStopResult);
                    }
                }
            }
        } catch (Exception e) {
            throw new RestHandlerException(e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
        }

        return CompletableFuture.completedFuture(new YarnJobStopResponseBody(list));
    }
}
