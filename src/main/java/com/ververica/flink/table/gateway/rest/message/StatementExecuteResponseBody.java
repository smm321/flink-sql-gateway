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

import com.ververica.flink.table.gateway.rest.result.ResultSet;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * {@link ResponseBody} for executing a statement.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class StatementExecuteResponseBody implements ResponseBody {

    private static final String FIELD_NAME_RESULT = "results";
    private static final String FIELD_NAME_STATEMENT_TYPE = "statement_types";

    @JsonProperty(FIELD_NAME_RESULT)
    private final List<ResultSet> results;

    @JsonProperty(FIELD_NAME_STATEMENT_TYPE)
    private final List<String> statementTypes;

    public StatementExecuteResponseBody(
            @JsonProperty(FIELD_NAME_RESULT) List<ResultSet> results,
            @JsonProperty(FIELD_NAME_STATEMENT_TYPE) List<String> statementTypes) {
        this.results = results;
        this.statementTypes = statementTypes;
    }

    public List<ResultSet> getResults() {
        return results;
    }

    public List<String> getStatementTypes() {
        return statementTypes;
    }

}
