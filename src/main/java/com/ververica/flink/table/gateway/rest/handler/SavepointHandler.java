package com.ververica.flink.table.gateway.rest.handler;

import com.ververica.flink.table.gateway.rest.entity.SavepointResult;
import com.ververica.flink.table.gateway.rest.message.SavepointRequestBody;
import com.ververica.flink.table.gateway.rest.message.SavepointResponseBody;
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
 * Handler for getting a job's info.
 */
public class SavepointHandler extends YarnJobAbstractHandler<SavepointRequestBody, SavepointResponseBody> {
    public SavepointHandler(Time timeout, Map responseHeaders,
                            MessageHeaders<SavepointRequestBody,
                                    SavepointResponseBody, EmptyMessageParameters> messageHeaders) {
        super(timeout, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture handleRequest(@Nonnull HandlerRequest<SavepointRequestBody> request) throws RestHandlerException {
        List result = new ArrayList();
        try {
            String jobName = request.getRequestBody().getJobName();
            List<ApplicationReport> reports = getApplicationReportsByName(jobName);
            if (CollectionUtils.isEmpty(reports)) {
                return CompletableFuture.completedFuture(new SavepointResponseBody("NOT JOB FOUND"));
            }

            if (CollectionUtils.size(reports) > 1) {
                return CompletableFuture.completedFuture(new SavepointResponseBody("MULTIPLE JOB FOUND"));
            }

            ApplicationId applicationId = reports.get(0).getApplicationId();
            if (null == applicationId) {
                return CompletableFuture.completedFuture(new SavepointResponseBody("JOB ALREADY DOWN"));
            }

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
                    String location = clusterClient.triggerSavepoint(jobId,
                            "hdfs:///checkpoint/" + jobName,
                            SavepointFormatType.CANONICAL).get();
                    SavepointResult savepointResult = new SavepointResult();
                    savepointResult.setAppId(applicationId.toString());
                    savepointResult.setJobId(jobId.toHexString());
                    savepointResult.setJobName(jobName);
                    savepointResult.setSavepointUrl(location);
                    result.add(savepointResult);
                }
            }
        } catch (Exception e) {
            throw new RestHandlerException(e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
        }
        return CompletableFuture.completedFuture(new SavepointResponseBody(result));
    }
}
