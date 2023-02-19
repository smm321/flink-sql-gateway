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

import com.ververica.flink.table.gateway.rest.entity.YarnJobStopResult;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * {@link ResponseBody} for canceling a Flink job.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class YarnJobStopResponseBody implements ResponseBody {

    private static final String FIELD_NAME_RRR_MSG = "errMsg";

    @JsonProperty(FIELD_NAME_RRR_MSG)
    private String errMsg;

    private static final String FIELD_NAME_DATA = "data";
    @JsonProperty(FIELD_NAME_DATA)
    private List<YarnJobStopResult> data;

    public YarnJobStopResponseBody(@JsonProperty(FIELD_NAME_DATA) List<YarnJobStopResult> data) {
        this.errMsg = "";
        this.data = data;
    }

    public YarnJobStopResponseBody(@JsonProperty(FIELD_NAME_RRR_MSG) String errMsg) {
        this.errMsg = errMsg;
    }

    public String getErrMsg() {
        return errMsg;
    }
}
