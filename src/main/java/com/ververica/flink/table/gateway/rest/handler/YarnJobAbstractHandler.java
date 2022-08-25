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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.Map;

/**
 * Yarn job abstract class.
 */
public abstract class YarnJobAbstractHandler<R extends RequestBody, P extends ResponseBody>
        extends AbstractRestHandler<R, P, EmptyMessageParameters> {

    protected YarnConfiguration yarnConfiguration;
    protected YarnClient yarnClient;

    public YarnJobAbstractHandler(
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<R, P, EmptyMessageParameters> messageHeaders) {

        super(timeout, responseHeaders, messageHeaders);
        try {
            yarnConfiguration = new YarnConfiguration();
            String dir = System.getProperty("yarn.conf.dir");
            yarnConfiguration.addResource(new Path(dir + "/yarn-site.xml"));
            yarnConfiguration.addResource(new Path(dir + "/core-site.xml"));
            yarnConfiguration.addResource(new Path(dir + "/hdfs-site.xml"));
            yarnClient = YarnClient.createYarnClient();
            yarnClient.init(yarnConfiguration);
            yarnClient.start();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
