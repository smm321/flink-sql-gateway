package com.ververica.flink.table.gateway.rest.handler;

import com.ververica.flink.table.gateway.rest.message.HdfsFileDownloadResponseBody;
import com.ververica.flink.table.gateway.rest.message.HdfsFileRequestBody;
import com.ververica.flink.table.gateway.utils.FutureUtils;
import com.ververica.flink.table.gateway.utils.ZipUtils;
import org.apache.commons.io.FileUtils;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.DefaultFileRegion;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.LastHttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpChunkedInput;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.stream.ChunkedFile;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Request handler for hdfs file download.
 */
public class HdfsFileDownloadHandler extends HdfsAbstractHandler<HdfsFileRequestBody, HdfsFileDownloadResponseBody>{
    protected final Logger log = LoggerFactory.getLogger(getClass());

    public HdfsFileDownloadHandler(Time timeout, Map<String, String> responseHeaders,
                                   MessageHeaders<HdfsFileRequestBody,
                                           HdfsFileDownloadResponseBody,
                                           EmptyMessageParameters> messageHeaders) {
        super(timeout, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture<HdfsFileDownloadResponseBody> handleRequest
            (@Nonnull HandlerRequest<HdfsFileRequestBody> request) throws RestHandlerException {
        return CompletableFuture.completedFuture(new HdfsFileDownloadResponseBody(null));
    }

    @Override
    protected CompletableFuture<Void> respondToRequest(ChannelHandlerContext ctx, HttpRequest httpRequest,
                                                       HandlerRequest<HdfsFileRequestBody> handlerRequest) {
        CompletableFuture<HdfsFileDownloadResponseBody> response;
        try {
            response = handleRequest(handlerRequest);
        } catch (RestHandlerException e) {
            response = FutureUtils.completedExceptionally(e);
        }

        return response.thenAccept(resp -> {
            try {
                transferFile(ctx, handlerRequest.getRequestBody().getPath(),  httpRequest);
            } catch (RestHandlerException e) {
                FutureUtils.completedExceptionally(e);
            }
        });

    }

    public void transferFile(ChannelHandlerContext ctx, String hdfsFile,HttpRequest httpRequest)
            throws RestHandlerException {

        Path hdfsPath = new Path(hdfsFile);
        String localFile = "downloadTmp/" + System.currentTimeMillis();
        String targetFile = "downloadTmp/" + System.currentTimeMillis() + ".zip";

        File file = new File(targetFile);
        try {
            // 复制文件到本地
            copyFileFromHdfs(hdfsPath, localFile, targetFile);
            downloadFile(ctx, httpRequest, targetFile, file);
        } catch (IOException var12) {
            throw new RestHandlerException("Could not transfer file " + targetFile + " to the client.",
                    HttpResponseStatus.INTERNAL_SERVER_ERROR,var12);
        } finally {
            try {
                file.delete();
            } catch (Exception ignored) {
            }
        }
    }

    private void downloadFile(ChannelHandlerContext ctx, HttpRequest httpRequest, String targetFile, File file)
            throws RestHandlerException, IOException {

        RandomAccessFile randomAccessFile;
        try {
            randomAccessFile = new RandomAccessFile(file, "r");
        } catch (FileNotFoundException var13) {
            log.error(var13.getMessage(),var13);
            throw new RestHandlerException("Can not find file " + targetFile + ".",
                    HttpResponseStatus.INTERNAL_SERVER_ERROR, var13);
        }

        long fileLength = randomAccessFile.length();
        FileChannel fileChannel = randomAccessFile.getChannel();

        try {
            HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            response.headers().set("Content-Type", "application/octet-stream");
            response.headers().set("Content-Disposition", "attachment; filename=\"" + file.getName() + "\"");
            if (HttpHeaders.isKeepAlive(httpRequest)) {
                response.headers().set("Connection", "keep-alive");
            }
            response.headers().set("content-length", fileLength);
            ctx.write(response);
            ChannelFuture lastContentFuture;
            if (ctx.pipeline().get(SslHandler.class) == null) {
                ctx.write(new DefaultFileRegion(fileChannel, 0L, fileLength), ctx.newProgressivePromise());
                lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            } else {
                lastContentFuture = ctx.writeAndFlush(
                        new HttpChunkedInput(new ChunkedFile(randomAccessFile, 0L, fileLength, 8192)),
                        ctx.newProgressivePromise()
                );
            }

            if (!HttpHeaders.isKeepAlive(httpRequest)) {
                lastContentFuture.addListener(ChannelFutureListener.CLOSE);
            }

        } catch (IOException var11) {
            try {
                randomAccessFile.close();
            } catch (IOException var10) {
                log.error(var10.getMessage(), var10);
                throw new RestHandlerException("Close file or channel error.",
                        HttpResponseStatus.INTERNAL_SERVER_ERROR, var10);
            }
        } finally {
            fileChannel.close();
        }
    }

    private void copyFileFromHdfs(Path hdfsPath, String localFile, String targetFile)
            throws RestHandlerException, IOException {
        try {
            downloadFolder(hdfsPath, new Path(localFile));
            ZipUtils.zipFolder(localFile, targetFile);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RestHandlerException(e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
        } finally {
            FileUtils.deleteDirectory(new File(localFile));
        }
    }

    public void downloadFolder(Path hdfsFolderPath, Path localFolderPath) throws IOException {
        FileStatus[] fileStatusArray = fs.listStatus(hdfsFolderPath);
        for (FileStatus fileStatus:fileStatusArray) {
            Path filePath = fileStatus.getPath();
            Path localFilePath = new Path(localFolderPath, filePath.getName());
            if (fileStatus.isDirectory()) {
                File folder = new File(localFilePath.toString());
                if (!folder.exists()) {
                    folder.mkdirs();
                }
                downloadFolder(filePath, localFilePath);
            } else {
                fs.copyToLocalFile(filePath, localFilePath);
            }
        }
    }

}
