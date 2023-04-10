package com.ververica.flink.table.gateway.rest.handler;

import com.ververica.flink.table.gateway.rest.entity.YarnJobStopResult;
import com.ververica.flink.table.gateway.rest.message.YarnJobStopRequestBody;
import com.ververica.flink.table.gateway.rest.message.YarnJobStopResponseBody;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Handler for canceling a Flink job with triggering savepoint gracefully.
 */
public class YarnJobStopHandler extends YarnJobAbstractHandler<YarnJobStopRequestBody, YarnJobStopResponseBody> {
    public YarnJobStopHandler(Time timeout, Map responseHeaders, MessageHeaders<YarnJobStopRequestBody,
            YarnJobStopResponseBody, EmptyMessageParameters> messageHeaders) {
        super(timeout, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture handleRequest(@Nonnull HandlerRequest<YarnJobStopRequestBody> request)
            throws RestHandlerException {
        String name = request.getRequestBody().getJobName();
        List<YarnJobStopResult> list = new ArrayList<>();
        try {
            List<ApplicationReport> reports = getApplicationReportsByName(name);
            if (CollectionUtils.isEmpty(reports)) {
                return CompletableFuture.completedFuture(new YarnJobStopResponseBody("NOT JOB FOUND"));
            }

            for (int i = 0; i < reports.size(); i++) {
                ApplicationId applicationId = reports.get(i).getApplicationId();
                Configuration configuration = new Configuration();
                configuration.set(YarnConfigOptions.APPLICATION_ID, applicationId.toString());
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
                                "hdfs:///checkpoint/" + jobName + "/" + jobId.toHexString(),
                                SavepointFormatType.CANONICAL).get();
                        YarnJobStopResult yarnJobStopResult = new YarnJobStopResult();
                        yarnJobStopResult.setAppId(applicationId.toString());
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
