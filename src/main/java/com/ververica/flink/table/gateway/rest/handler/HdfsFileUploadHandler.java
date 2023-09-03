package com.ververica.flink.table.gateway.rest.handler;

import com.ververica.flink.table.gateway.rest.message.HdfsFileUploadRequestBody;
import com.ververica.flink.table.gateway.rest.message.HdfsFileUploadResponseBody;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Request handler for hdfs file upload.
 * todo
 */
public class HdfsFileUploadHandler extends HdfsAbstractHandler<HdfsFileUploadRequestBody, HdfsFileUploadResponseBody>{
    public HdfsFileUploadHandler(Time timeout, Map<String, String> responseHeaders,
                                 MessageHeaders<HdfsFileUploadRequestBody, HdfsFileUploadResponseBody,
                                         EmptyMessageParameters> messageHeaders) {
        super(timeout, responseHeaders, messageHeaders);
    }

    /**
     * This method is called for every incoming request and returns a {@link CompletableFuture} containing a the response.
     *
     * <p>Implementations may decide whether to throw {@link RestHandlerException}s or fail the returned
     * {@link CompletableFuture} with a {@link RestHandlerException}.
     *
     * <p>Failing the future with another exception type or throwing unchecked exceptions is regarded as an
     * implementation error as it does not allow us to provide a meaningful HTTP status code. In this case a
     * {@link HttpResponseStatus#INTERNAL_SERVER_ERROR} will be returned.
     *
     * @param request request that should be handled
     * @return future containing a handler response
     * @throws RestHandlerException if the handling failed
     */
    @Override
    protected CompletableFuture<HdfsFileUploadResponseBody> handleRequest(
            @Nonnull HandlerRequest<HdfsFileUploadRequestBody> request) throws RestHandlerException {
        File file = request.getRequestBody().getFile();
        return null;
    }
}
