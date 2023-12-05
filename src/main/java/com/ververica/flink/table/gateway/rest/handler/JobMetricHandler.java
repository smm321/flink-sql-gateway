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

package com.ververica.flink.table.gateway.rest.handler;

import com.ververica.flink.table.gateway.rest.message.JobMetricRequestBody;
import com.ververica.flink.table.gateway.rest.message.JobMetricResponseBody;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Request handler for canceling a Flink job.
 */
public class JobMetricHandler
        extends AbstractRestHandler<JobMetricRequestBody, JobMetricResponseBody, EmptyMessageParameters> {

    private static final RequestConfig restConfig = RequestConfig
                .custom()
                .setSocketTimeout(3000)
                .setConnectTimeout(3000)
                .setConnectionRequestTimeout(3000)
                .build();

    public JobMetricHandler(
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<JobMetricRequestBody, JobMetricResponseBody, EmptyMessageParameters> messageHeaders) {

        super(timeout, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture<JobMetricResponseBody> handleRequest(
            @Nonnull HandlerRequest<JobMetricRequestBody> request) throws RestHandlerException {

        String url = request.getRequestBody().getUrl();
        String content = "";
        try {
            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpUriRequest restRequest = RequestBuilder.get(url)
                    .addHeader("Content-Type", "application/json")
                    .setConfig(restConfig)
                    .build();

            CloseableHttpResponse response = httpClient.execute(restRequest);
            content = EntityUtils.toString(response.getEntity());

        } catch (Exception e) {
            throw new RestHandlerException(e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
        }

        return CompletableFuture.completedFuture(new JobMetricResponseBody(content));
    }
}
