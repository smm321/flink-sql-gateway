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

import com.ververica.flink.table.gateway.utils.FutureUtils;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Super class for netty-based handlers that work with {@link RequestBody}s and {@link ResponseBody}s.
 *
 * <p>Subclasses must be thread-safe.
 *
 * <p>This class is copied from {@link org.apache.flink.runtime.rest.handler.AbstractRestHandler},
 * and does not support leader gateway.
 *
 * @param <R> type of incoming requests
 * @param <P> type of outgoing responses
 */
@ChannelHandler.Sharable
public abstract class AbstractRestHandler<R extends RequestBody, P extends ResponseBody, M extends MessageParameters>
        extends AbstractHandler<R, M> {

    private final MessageHeaders<R, P, M> messageHeaders;

    protected AbstractRestHandler(
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<R, P, M> messageHeaders) {
        super(timeout, responseHeaders, messageHeaders);
        this.messageHeaders = Preconditions.checkNotNull(messageHeaders);
    }

    public MessageHeaders<R, P, M> getMessageHeaders() {
        return messageHeaders;
    }

    @Override
    protected CompletableFuture<Void> respondToRequest(ChannelHandlerContext ctx, HttpRequest httpRequest,
                                                       HandlerRequest<R> handlerRequest) {
        CompletableFuture<P> response;

        try {
            response = handleRequest(handlerRequest);
        } catch (RestHandlerException e) {
            response = FutureUtils.completedExceptionally(e);
        }

        return response.thenAccept(resp -> HandlerUtils
                .sendResponse(ctx, httpRequest, resp, messageHeaders.getResponseStatusCode(), responseHeaders));
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
    protected abstract CompletableFuture<P> handleRequest(@Nonnull HandlerRequest<R> request)
            throws RestHandlerException;
}
