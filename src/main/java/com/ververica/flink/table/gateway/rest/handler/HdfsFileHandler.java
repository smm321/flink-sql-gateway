package com.ververica.flink.table.gateway.rest.handler;

import com.ververica.flink.table.gateway.rest.message.HdfsFileRequestBody;
import com.ververica.flink.table.gateway.rest.message.HdfsFileResponseBody;
import com.ververica.flink.table.gateway.rest.result.HdfsFileInfo;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Request handler for hdfs file.
 */
public class HdfsFileHandler extends HdfsAbstractHandler<HdfsFileRequestBody, HdfsFileResponseBody>{

    public HdfsFileHandler(Time timeout, Map<String, String> responseHeaders,
                           MessageHeaders<HdfsFileRequestBody, HdfsFileResponseBody, EmptyMessageParameters> messageHeaders) {
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
    protected CompletableFuture<HdfsFileResponseBody> handleRequest(@Nonnull HandlerRequest<HdfsFileRequestBody> request)
            throws RestHandlerException {
        List<HdfsFileInfo> results = new ArrayList<>();
        try {

            FileStatus[] fileStatusList = fs.listStatus(new Path(Objects.requireNonNull(request.getRequestBody().getPath())));
            for (FileStatus fileStatus : fileStatusList) {
                String path = fileStatus.getPath().toString();
                long accessTime = fileStatus.getModificationTime();
                SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String time = sf.format(new Date(accessTime));

                HdfsFileInfo hdfsFileInfo = new HdfsFileInfo(path,time);
                results.add(hdfsFileInfo);
            }

            Collections.sort(results, new Comparator<HdfsFileInfo>() {
                @Override
                public int compare(HdfsFileInfo o1, HdfsFileInfo o2) {
                    return o2.getFileTime().compareTo(o1.getFileTime());
                }
            });

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        return  CompletableFuture.completedFuture(new HdfsFileResponseBody(results));
    }
}
