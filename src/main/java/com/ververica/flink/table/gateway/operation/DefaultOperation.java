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

package com.ververica.flink.table.gateway.operation;

import com.ververica.flink.table.gateway.context.ExecutionContext;
import com.ververica.flink.table.gateway.context.SessionContext;
import com.ververica.flink.table.gateway.rest.result.ColumnInfo;
import com.ververica.flink.table.gateway.rest.result.ConstantNames;
import com.ververica.flink.table.gateway.rest.result.ResultKind;
import com.ververica.flink.table.gateway.rest.result.ResultSet;
import com.ververica.flink.table.gateway.utils.SqlExecutionException;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

/**
 * Operation for Default command.
 */
public class DefaultOperation implements NonJobOperation {
	private final ExecutionContext<?> context;
	private final String statement;

	public DefaultOperation(SessionContext context, String statement) {
		this.context = context.getExecutionContext();
		this.statement = statement;
	}

	@Override
	public ResultSet execute() {
		final TableEnvironment tableEnv = context.getTableEnvironment();
		StringBuffer response = new StringBuffer();
		context.wrapClassLoader(() -> {
			try {

				TableResult result = tableEnv.executeSql(statement);
				result.collect().forEachRemaining(response::append);
				return null;
			} catch (CatalogException e) {
				throw new SqlExecutionException("Failed to execute statement " + statement, e);
			}
		});

		return ResultSet.builder()
				.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
				.columns(ColumnInfo.create(ConstantNames.UNPARSED_STATEMENT, new VarCharType(true, response.length())))
				.data(Row.of(response))
				.build();
	}
}
