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
import com.ververica.flink.table.gateway.rest.entity.LineageResult;
import com.ververica.flink.table.gateway.rest.result.ColumnInfo;
import com.ververica.flink.table.gateway.rest.result.ConstantNames;
import com.ververica.flink.table.gateway.rest.result.ResultKind;
import com.ververica.flink.table.gateway.rest.result.ResultSet;
import com.ververica.flink.table.gateway.utils.FlinkUtil;
import com.ververica.flink.table.gateway.utils.SqlExecutionException;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.hadoop.hdfs.web.JsonUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Operation for PARSE LINEAGE command.
 */
public class ParseLineageOperation implements NonJobOperation {
	private final ExecutionContext<?> context;
	private final String statement;
	private final String param;
	private SqlParser.Config config;

	public ParseLineageOperation(SessionContext context, String statement, String param) {
		this.context = context.getExecutionContext();
		this.statement = statement;
		this.config = SqlParser.configBuilder()
				.setParserFactory(FlinkSqlParserImpl.FACTORY)
				.setConformance(FlinkSqlConformance.DEFAULT)
				.setLex(Lex.JAVA)
				.setIdentifierMaxLength(256)
				.build();
		this.param = param;
	}

	@Override
	public ResultSet execute() {
		try {
			StreamTableEnvironmentImpl impl = context.getStreamTableEnvironmentImpl();
			FlinkUtil.setConfig(param, context.getStreamTableEnvironmentImpl());
			LineageResult lineageResult = new LineageResult();

			List<Operation> operation = impl.getParser().parse(statement);
			if (operation.size() != 1){
				throw new TableException("Only single statement is supported.");
			}

			Operation op = operation.get(0);
			if (!(op instanceof SinkModifyOperation)){
				throw new TableException("Only insert statement is supported.");
			}

			Map<Integer, String> sinkColumn = new HashMap<>();
			SqlParser sqlParser = SqlParser.create(statement, config);
			RichSqlInsert richSqlInsert = (RichSqlInsert)sqlParser.parseStmt();
			for (int i = 0; i < richSqlInsert.getTargetColumnList().getList().size(); i++){
				//collect table sink columns info
				//todo should be enhancement
				sinkColumn.put(i, richSqlInsert.getTargetColumnList().getList().get(i).toString());
			}

			List<ModifyOperation> operations = new ArrayList<>();
			operations.add((ModifyOperation)op);

			SinkModifyOperation sinkModifyOperation = (SinkModifyOperation)op;
			lineageResult.setSinkDbName(sinkModifyOperation.getContextResolvedTable().getIdentifier()
					.getDatabaseName());
			lineageResult.setSinkTableName(sinkModifyOperation.getContextResolvedTable().getIdentifier()
					.getObjectName());
			//todo extract partition information in the future
			PlannerQueryOperation queryOperation = (PlannerQueryOperation)sinkModifyOperation.getChild();
			RelNode relNode = queryOperation.getCalciteTree();
			lineageResult.setSourceTable(findSourceTable(relNode, new ArrayList()));
			RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();
			for (int i = 0; i < queryOperation.getResolvedSchema().getColumnCount(); i++){
				Set<RelColumnOrigin> origins = mq.getColumnOrigins(relNode, i);
				if (CollectionUtils.isEmpty(origins)){
					continue;
				}
				int finalI = i;
				Map<String, List<Tuple3<String, String, String>>> element = new HashMap<>();
				origins.forEach(o -> {
					if (element.containsKey(sinkColumn.get(finalI))){
						List<Tuple3<String, String, String>> list = element.get(sinkColumn.get(finalI));
						list.add(Tuple3.of(o.getOriginTable().getQualifiedName().get(1),
								           o.getOriginTable().getQualifiedName().get(2),
								           o.getOriginTable().getRowType().getFieldNames()
												   .get(o.getOriginColumnOrdinal())));
					} else {
						element.put(sinkColumn.get(finalI),
								Stream.of(Tuple3.of(o.getOriginTable().getQualifiedName().get(1),
										  o.getOriginTable().getQualifiedName().get(2),
										  o.getOriginTable().getRowType().getFieldNames()
												  .get(o.getOriginColumnOrdinal()))
										).collect(Collectors.toList())
								);
					}
				});
				lineageResult.getSourceColumn().add(element);
			}

			for (String func : context.getFunctionCatalog().getUserDefinedFunctions()){
				context.getFunctionCatalog().dropTempCatalogFunction(ObjectIdentifier.of(
						context.getTableEnvironment().getCurrentCatalog(),
						context.getTableEnvironment().getCurrentDatabase(), func),
						true);
			}
			return ResultSet.builder()
				.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
				.columns(ColumnInfo.create(ConstantNames.PARSE_LINEAGE, new VarCharType(false,
						JsonUtil.toJsonString(lineageResult).length())))
				.data(Row.of(JsonUtil.toJsonString(lineageResult)))
				.build();
		} catch (Throwable t) {
			// catch everything such that the query does not crash the executor
			throw new SqlExecutionException("Invalid SQL statement.", t);
		}
	}

	private List<Tuple2<String, String>> findSourceTable(RelNode optRelNode, List list){
		List<RelNode> inputs = optRelNode.getInputs();
		//leaf node of the tree
		if (CollectionUtils.isEmpty(inputs)){
			RelOptTable table = optRelNode.getTable();
			if (null != table){
				list.add(Tuple2.of(table.getQualifiedName().get(1), table.getQualifiedName().get(2)));
			}
//			return list;
		}
		Iterator iterator = inputs.listIterator();
		while (iterator.hasNext()) {
			findSourceTable((RelNode)iterator.next(), list);
		}
		return list;
	}
}
