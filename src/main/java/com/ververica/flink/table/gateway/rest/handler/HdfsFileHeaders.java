package com.ververica.flink.table.gateway.rest.handler;

import com.ververica.flink.table.gateway.rest.message.HdfsFileRequestBody;
import com.ververica.flink.table.gateway.rest.message.HdfsFileResponseBody;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.runtime.rest.versioning.RuntimeRestAPIVersion;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;
import java.util.Collections;

/**
 * Message headers for hdfs file.
 */
public class HdfsFileHeaders
        implements MessageHeaders<HdfsFileRequestBody, HdfsFileResponseBody, EmptyMessageParameters> {

    private static final HdfsFileHeaders INSTANCE = new HdfsFileHeaders();

    public static final String URL = "/hdfs/file";

    private HdfsFileHeaders() {
    }

    @Override
    public Class<HdfsFileResponseBody> getResponseClass() {
        return HdfsFileResponseBody.class;
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
    public Class<HdfsFileRequestBody> getRequestClass() {
        return HdfsFileRequestBody.class;
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

    public static HdfsFileHeaders getInstance() {
        return INSTANCE;
    }

}
