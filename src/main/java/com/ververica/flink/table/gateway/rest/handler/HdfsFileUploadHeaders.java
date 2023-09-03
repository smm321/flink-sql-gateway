package com.ververica.flink.table.gateway.rest.handler;

import com.ververica.flink.table.gateway.rest.message.HdfsFileUploadRequestBody;
import com.ververica.flink.table.gateway.rest.message.HdfsFileUploadResponseBody;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.runtime.rest.versioning.RuntimeRestAPIVersion;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;
import java.util.Collections;

/**
 * Message headers for hdfs file upload.
 */
public class HdfsFileUploadHeaders  implements MessageHeaders<HdfsFileUploadRequestBody,
        HdfsFileUploadResponseBody, EmptyMessageParameters> {

    private static final HdfsFileUploadHeaders INSTANCE = new HdfsFileUploadHeaders();

    public static final String URL = "/hdfs/file/upload";

    private HdfsFileUploadHeaders() {
    }

    @Override
    public Class<HdfsFileUploadResponseBody> getResponseClass() {
        return HdfsFileUploadResponseBody.class;
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
    public Class<HdfsFileUploadRequestBody> getRequestClass() {
        return HdfsFileUploadRequestBody.class;
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

    public static HdfsFileUploadHeaders getInstance() {
        return INSTANCE;
    }
}
