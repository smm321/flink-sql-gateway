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

import com.ververica.flink.table.gateway.rest.message.JarUploadRequestBody;
import com.ververica.flink.table.gateway.rest.message.JarUploadResponseBody;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;


/** Handles .jar file uploads. */
public class JarUploadHandler
        extends AbstractRestHandler<JarUploadRequestBody, JarUploadResponseBody, EmptyMessageParameters> {

    public JarUploadHandler(
            final Time timeout,
            final Map<String, String> responseHeaders,
            final MessageHeaders<JarUploadRequestBody, JarUploadResponseBody, EmptyMessageParameters>
                    messageHeaders) {
        super(timeout, responseHeaders, messageHeaders);
    }

    @Override
    @VisibleForTesting
    public CompletableFuture<JarUploadResponseBody> handleRequest(
            @Nonnull final HandlerRequest<JarUploadRequestBody> request)
            throws RestHandlerException {
        Collection<File> uploadedFiles = request.getUploadedFiles();
        String jarDir = request.getRequestBody().getJarDir();
        if (uploadedFiles.size() != 1) {
            throw new RestHandlerException(
                    "Exactly 1 file must be sent, received " + uploadedFiles.size() + '.',
                    HttpResponseStatus.BAD_REQUEST);
        }
        final Path fileUpload = uploadedFiles.iterator().next().toPath();
        return CompletableFuture.supplyAsync(
                () -> {
                    if (!fileUpload.getFileName().toString().endsWith(".jar")) {
                        throw new CompletionException(
                                new RestHandlerException(
                                        "Only Jar files are allowed.",
                                        HttpResponseStatus.BAD_REQUEST));
                    } else {
                        final Path destination;
                        if (jarDir.endsWith(File.separator)) {
                            destination = fileUpload.resolve(jarDir + fileUpload.getFileName());
                        } else {
                            destination = fileUpload.resolve(jarDir + File.separator + fileUpload.getFileName());
                        }

                        try {
                            if (Files.exists(destination)) {
                                Files.move(destination, destination.resolveSibling(fileUpload.getFileName()
                                        + String.valueOf(System.currentTimeMillis() / 1000)),
                                        StandardCopyOption.REPLACE_EXISTING);
                            } else {
                                Files.createDirectories(destination);
                            }
                            Files.copy(fileUpload, destination, StandardCopyOption.REPLACE_EXISTING);
                            Set<PosixFilePermission> perms = new HashSet<>();
                            perms.add(PosixFilePermission.OWNER_READ);
                            perms.add(PosixFilePermission.OWNER_WRITE);
                            perms.add(PosixFilePermission.GROUP_EXECUTE);
                            Files.setPosixFilePermissions(destination, perms);
                        } catch (IOException e) {
                            throw new CompletionException(
                                    new RestHandlerException(
                                            String.format(
                                                    "Could not move uploaded jar file [%s] to [%s].",
                                                    fileUpload, destination),
                                            HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                            e));
                        }
                        return new JarUploadResponseBody(destination.normalize().toString());
                    }
                });
    }
}
