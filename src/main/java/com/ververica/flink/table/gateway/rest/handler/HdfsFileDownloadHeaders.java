package com.ververica.flink.table.gateway.rest.handler;

import com.ververica.flink.table.gateway.rest.message.HdfsFileDownloadResponseBody;
import com.ververica.flink.table.gateway.rest.message.HdfsFileRequestBody;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.runtime.rest.versioning.RuntimeRestAPIVersion;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;
import java.util.Collections;

/**
 * Message headers for hdfs file download.
 */
public class HdfsFileDownloadHeaders
        implements MessageHeaders<HdfsFileRequestBody, HdfsFileDownloadResponseBody, EmptyMessageParameters> {

    private static final HdfsFileDownloadHeaders INSTANCE = new HdfsFileDownloadHeaders();

    public static final String URL = "/hdfs/file/download";

    private HdfsFileDownloadHeaders() {
    }

    @Override
    public Class<HdfsFileDownloadResponseBody> getResponseClass() {
        return HdfsFileDownloadResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String getDescription() {
        return "Closes the specific session.";
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
        return HttpMethodWrapper.GET;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    @Override
    public Collection<? extends RestAPIVersion<?>> getSupportedAPIVersions() {
        return Collections.singleton(RuntimeRestAPIVersion.V1);
    }

    public static HdfsFileDownloadHeaders getInstance() {
        return INSTANCE;
    }
}
