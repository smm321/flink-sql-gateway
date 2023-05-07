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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.RuleSets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.calcite.RexFactory;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkDecorrelateProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkGroupProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.FlinkRelTimeIndicatorProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkVolcanoProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;
import org.apache.flink.table.planner.plan.rules.FlinkStreamRuleSets;
import org.apache.flink.table.planner.plan.rules.logical.EventTimeTemporalJoinRewriteRule;
import org.apache.flink.table.planner.plan.trait.MiniBatchInterval;
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
	private FlinkChainedProgram chainedProgram;
	private static final String SUBQUERY_REWRITE = "subquery_rewrite";
	private static final String TEMPORAL_JOIN_REWRITE = "temporal_join_rewrite";
	private static final String DECORRELATE = "decorrelate";
	private static final String TIME_INDICATOR = "time_indicator";
	private static final String DEFAULT_REWRITE = "default_rewrite";
	private static final String PREDICATE_PUSHDOWN = "predicate_pushdown";
	private static final String JOIN_REORDER = "join_reorder";
	private static final String PROJECT_REWRITE = "project_rewrite";
	private static final String LOGICAL = "logical";
	private static final String LOGICAL_REWRITE = "logical_rewrite";

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
		chainedProgram = new FlinkChainedProgram();
		chainedProgram.addLast(SUBQUERY_REWRITE,
				FlinkGroupProgramBuilder.newBuilder()
						.addProgram(FlinkHepRuleSetProgramBuilder.newBuilder()
								.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
								.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
								.add(FlinkStreamRuleSets.TABLE_REF_RULES())
								.build(),  "convert table references before rewriting sub-queries to semi-join")
						.addProgram(FlinkHepRuleSetProgramBuilder.newBuilder()
								.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
								.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
								.add(FlinkStreamRuleSets.SEMI_JOIN_RULES())
								.build(), "rewrite sub-queries to semi-join")
						.addProgram(FlinkHepRuleSetProgramBuilder.newBuilder()
								.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION())
								.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
								.add(FlinkStreamRuleSets.TABLE_SUBQUERY_RULES())
								.build(), "sub-queries remove")
						.addProgram(FlinkHepRuleSetProgramBuilder.newBuilder()
								.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
								.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
								.add(FlinkStreamRuleSets.TABLE_REF_RULES())
								.build(), "convert table references after sub-queries removed")
						.build());

		chainedProgram.addLast(TEMPORAL_JOIN_REWRITE,
				FlinkGroupProgramBuilder.newBuilder()
						.addProgram(FlinkHepRuleSetProgramBuilder.newBuilder()
								.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
								.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
								.add(FlinkStreamRuleSets.EXPAND_PLAN_RULES())
								.build(), "convert correlate to temporal table join")
						.addProgram(FlinkHepRuleSetProgramBuilder.newBuilder()
								.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
								.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
								.add(FlinkStreamRuleSets.POST_EXPAND_CLEAN_UP_RULES())
								.build(), "convert enumerable table scan")
						.build());

		chainedProgram.addLast(DECORRELATE,
				FlinkGroupProgramBuilder.newBuilder()
						// rewrite before decorrelation
						.addProgram(
								FlinkHepRuleSetProgramBuilder.newBuilder()
										.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
										.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
										.add(FlinkStreamRuleSets.PRE_DECORRELATION_RULES())
										.build(), "pre-rewrite before decorrelation")
						.addProgram(new FlinkDecorrelateProgram(), "")
						.build());

//		// convert time indicators
//		chainedProgram.addLast(TIME_INDICATOR, new FlinkRelTimeIndicatorProgram());
////
//		// default rewrite, includes: predicate simplification, expression reduction, window
//		// properties rewrite, etc.
		chainedProgram.addLast(
				DEFAULT_REWRITE,
				FlinkHepRuleSetProgramBuilder.newBuilder()
						.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
						.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
						.add(FlinkStreamRuleSets.DEFAULT_REWRITE_RULES())
						.build());

		chainedProgram.addLast(
				PREDICATE_PUSHDOWN,
				FlinkGroupProgramBuilder
						.newBuilder()
						.addProgram(
							FlinkGroupProgramBuilder
									.newBuilder()
									.addProgram(
											FlinkHepRuleSetProgramBuilder
												.newBuilder()
												.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
												.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
												.add(FlinkStreamRuleSets.JOIN_PREDICATE_REWRITE_RULES())
												.build(),
									"join predicate rewrite"
							)
						.addProgram(
									FlinkHepRuleSetProgramBuilder.newBuilder()
											.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION())
											.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
											.add(FlinkStreamRuleSets.FILTER_PREPARE_RULES())
											.build(),
								"filter rules"
						)
						.setIterations(5)
						.build(),
				"predicate rewrite"
		).addProgram(
				FlinkGroupProgramBuilder
						.newBuilder()
						.addProgram(
								FlinkHepRuleSetProgramBuilder.newBuilder()
										.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
										.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
										.add(FlinkStreamRuleSets.PUSH_PARTITION_DOWN_RULES())
										.build(),
								"push down partitions into table scan"
						)
						.addProgram(
								FlinkHepRuleSetProgramBuilder.newBuilder()
										.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
										.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
										.add(FlinkStreamRuleSets.PUSH_FILTER_DOWN_RULES())
										.build(),
								"push down filters into table scan"
						)
						.build(),
				"push predicate into table scan"
			).addProgram(
					FlinkHepRuleSetProgramBuilder.newBuilder()
							.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
							.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
							.add(FlinkStreamRuleSets.PRUNE_EMPTY_RULES())
							.build(),
					"prune empty after predicate push down"
			)
			.build()
		);

		// join reorder
		if (context.getExecutionContext().getFlinkConfig()
				.get(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED)) {
			chainedProgram.addLast(
					JOIN_REORDER,
					FlinkGroupProgramBuilder.newBuilder()
							.addProgram(FlinkHepRuleSetProgramBuilder.newBuilder()
									.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION())
									.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
									.add(FlinkStreamRuleSets.JOIN_REORDER_PREPARE_RULES())
									.build(), "merge join into MultiJoin")
							.addProgram(FlinkHepRuleSetProgramBuilder.newBuilder()
									.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
									.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
									.add(FlinkStreamRuleSets.JOIN_REORDER_RULES())
									.build(), "do join reorder")
							.build());
		}

		// project rewrite
		chainedProgram.addLast(
				PROJECT_REWRITE,
				FlinkHepRuleSetProgramBuilder.newBuilder()
						.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION())
						.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
						.add(FlinkStreamRuleSets.PROJECT_RULES())
						.build());

		RelTrait[] relTraits = new RelTrait[1];
		relTraits[0] = FlinkConventions.LOGICAL();
		chainedProgram.addLast(LOGICAL, FlinkVolcanoProgramBuilder.newBuilder()
				.add(FlinkStreamRuleSets.LOGICAL_OPT_RULES())
				.setRequiredOutputTraits(relTraits)
				.build());



		chainedProgram.addLast(LOGICAL_REWRITE, FlinkGroupProgramBuilder.newBuilder()
				.addProgram(
						FlinkHepRuleSetProgramBuilder.newBuilder()
						.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
						.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
						.add(FlinkStreamRuleSets.LOGICAL_REWRITE())
						.build(), "")
				.addProgram(
						FlinkHepRuleSetProgramBuilder.newBuilder()
						.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
						.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
						.add(RuleSets.ofList(new RelOptRule[]{
								EventTimeTemporalJoinRewriteRule.Config.JOIN_CALC_SNAPSHOT_CALC_WMA_CALC_TS.toRule(),
								EventTimeTemporalJoinRewriteRule.Config.JOIN_CALC_SNAPSHOT_CALC_WMA_TS.toRule(),
								EventTimeTemporalJoinRewriteRule.Config.JOIN_CALC_SNAPSHOT_WMA_CALC_TS.toRule(),
								EventTimeTemporalJoinRewriteRule.Config.JOIN_CALC_SNAPSHOT_WMA_TS.toRule(),
								EventTimeTemporalJoinRewriteRule.Config.JOIN_SNAPSHOT_CALC_WMA_CALC_TS.toRule(),
								EventTimeTemporalJoinRewriteRule.Config.JOIN_SNAPSHOT_CALC_WMA_TS.toRule(),
								EventTimeTemporalJoinRewriteRule.Config.JOIN_SNAPSHOT_WMA_CALC_TS.toRule(),
								EventTimeTemporalJoinRewriteRule.Config.JOIN_SNAPSHOT_WMA_TS.toRule()}))
						.build(), "")
				.build()
		);

		// convert time indicators
		chainedProgram.addLast(TIME_INDICATOR, new FlinkRelTimeIndicatorProgram());
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
			lineageResult.setSinkDbName(sinkModifyOperation.getContextResolvedTable().getIdentifier().getDatabaseName());
			lineageResult.setSinkTableName(sinkModifyOperation.getContextResolvedTable().getIdentifier().getObjectName());
			//todo extract partition information in the future
			PlannerQueryOperation queryOperation = (PlannerQueryOperation)sinkModifyOperation.getChild();
			RelNode relNode = queryOperation.getCalciteTree();
			RelNode optRelNode = chainedProgram.optimize(relNode, new StreamOptimizeContext() {
				@Override
				public boolean isUpdateBeforeRequired() {
					return false;
				}

				@Override
				public MiniBatchInterval getMiniBatchInterval() {
					return MiniBatchInterval.NONE;
				}

				@Override
				public FlinkRelBuilder getFlinkRelBuilder() {
					PlannerBase base = (PlannerBase)impl.getPlanner();
					return base.createRelBuilder();
				}

				@Override
				public boolean needFinalTimeIndicatorConversion() {
					return true;
				}

				@Override
				public FunctionCatalog getFunctionCatalog() {
					return context.getFunctionCatalog();
				}

				@Override
				public CatalogManager getCatalogManager() {
					return impl.getCatalogManager();
				}

				@Override
				public ModuleManager getModuleManager() {
					return null;
				}

				@Override
				public RexFactory getRexFactory() {
					return null;
				}

				@Override
				public boolean isBatchMode() {
					return false;
				}

				@Override
				public ClassLoader getClassLoader() {
					PlannerBase base = (PlannerBase)impl.getPlanner();
					return base.getFlinkContext().getClassLoader();
				}

				@Override
				public TableConfig getTableConfig() {
					return impl.getConfig();
				}

				@Override
				public <C> C unwrap(Class<C> clazz) {
					return clazz.isInstance(this) ? clazz.cast(this) : null;
				}
			});

			lineageResult.setSourceTable(findSourceTable(optRelNode, new ArrayList()));
			RelMetadataQuery mq = optRelNode.getCluster().getMetadataQuery();
			for (int i = 0; i < queryOperation.getResolvedSchema().getColumnCount(); i++){
				Set<RelColumnOrigin> origins = mq.getColumnOrigins(optRelNode, i);
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
								           o.getOriginTable().getRowType().getFieldNames().get(o.getOriginColumnOrdinal())));
					} else {
						element.put(sinkColumn.get(finalI),
								Stream.of(Tuple3.of(o.getOriginTable().getQualifiedName().get(1),
										  o.getOriginTable().getQualifiedName().get(2),
										  o.getOriginTable().getRowType().getFieldNames().get(o.getOriginColumnOrdinal()))
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
