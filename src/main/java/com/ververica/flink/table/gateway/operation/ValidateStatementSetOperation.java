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
import com.ververica.flink.table.gateway.utils.FlinkUtil;
import com.ververica.flink.table.gateway.utils.SqlExecutionException;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.delegation.StreamExecutor;
import org.apache.flink.table.planner.utils.ExecutorUtils;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * Operation for VALIDATE command.
 * validate statement set sqls only
 */
public class ValidateStatementSetOperation implements NonJobOperation {
	private final ExecutionContext<?> context;
	private final String statement;
	private final String param;

	public ValidateStatementSetOperation(SessionContext context, String statement, String param) {
		this.context = context.getExecutionContext();
		this.statement = statement;
		this.param = param;
	}

	@Override
	public ResultSet execute() {
		try {
			List<ModifyOperation> operations = new ArrayList<>();
			StreamTableEnvironmentImpl impl = context.getStreamTableEnvironmentImpl();
			for (String sql : statement.split(";")){
				List<Operation> operation = impl.getParser().parse(sql);

				if (operation.size() != 1){
					throw new TableException("Only single statement is supported.");
				}
				Operation op = operation.get(0);
				if (!(op instanceof ModifyOperation)){
					throw new TableException("Only insert statement is supported.");
				} else {
					operations.add((ModifyOperation)op);
				}
			}
			FlinkUtil.setConfig(param, impl);
			List<Transformation<?>> transformations = impl.getPlanner().translate(operations);
			StreamGraph streamGraph;
			if (context.getExecutor() instanceof StreamExecutor){
				StreamExecutor executor = (StreamExecutor)context.getExecutor();
				streamGraph = ExecutorUtils.generateStreamGraph(executor.getExecutionEnvironment(), transformations);
				if (context.getFlinkConfig().containsKey(PipelineOptions.NAME.key())){
					streamGraph.setJobName(context.getFlinkConfig().getString(PipelineOptions.NAME));
				}
			} else {
				throw new TableException(String.format("Supported streaming mode only"));
			}

			return ResultSet.builder()
				.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
				.columns(ColumnInfo.create(ConstantNames.EXPLAIN_RESULT, new VarCharType(false,
						streamGraph.getStreamingPlanAsJSON().length())))
				.data(Row.of(streamGraph.getStreamingPlanAsJSON()))
				.build();
		} catch (Throwable t) {
			// catch everything such that the query does not crash the executor
			throw new SqlExecutionException("Invalid SQL statement.", t);
		}
	}
}
