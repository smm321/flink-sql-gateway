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

package com.ververica.flink.table.gateway.rest.message;

import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

/** {@link ResponseBody} for {@link com.ververica.flink.table.gateway.rest.handler.JarUploadHandler}. */
public class JarUploadRequestBody implements RequestBody {

    private static final String JAR_DIR = "jar_dir";

    @JsonProperty(JAR_DIR)
    private final String jarDir;

    @JsonCreator
    public JarUploadRequestBody(@JsonProperty(JAR_DIR) final String jarDir) {
        this.jarDir = requireNonNull(jarDir);
    }

    public String getJarDir() {
        return jarDir;
    }
}
