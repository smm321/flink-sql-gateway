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

import com.ververica.flink.table.gateway.rest.message.YarnJobSubmitRequestBody;
import com.ververica.flink.table.gateway.rest.message.YarnJobSubmitResponseBody;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.runtime.rest.versioning.RuntimeRestAPIVersion;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;
import java.util.Collections;

/**
 * Message headers for canceling a Flink job.
 */
public class YarnJobSubmitHeaders
        implements MessageHeaders<YarnJobSubmitRequestBody, YarnJobSubmitResponseBody, EmptyMessageParameters> {

    private static final YarnJobSubmitHeaders INSTANCE = new YarnJobSubmitHeaders();

    public static final String URL = "/job/submit";

    private YarnJobSubmitHeaders() {
    }

    @Override
    public Class<YarnJobSubmitResponseBody> getResponseClass() {
        return YarnJobSubmitResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String getDescription() {
        return "Submit a running job.";
    }

    @Override
    public Class<YarnJobSubmitRequestBody> getRequestClass() {
        return YarnJobSubmitRequestBody.class;
    }

    @Override
    public EmptyMessageParameters getUnresolvedMessageParameters() {
        return EmptyMessageParameters.getInstance();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.POST;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    @Override
    public Collection<? extends RestAPIVersion<?>> getSupportedAPIVersions() {
        return Collections.singleton(RuntimeRestAPIVersion.V1);
    }

    public static YarnJobSubmitHeaders getInstance() {
        return INSTANCE;
    }

}
