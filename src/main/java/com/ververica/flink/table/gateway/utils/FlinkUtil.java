package com.ververica.flink.table.gateway.utils;

import com.google.gson.Gson;
import com.ververica.flink.table.gateway.rest.entity.FlinkJob;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * flink util.
 */
@Slf4j
public class FlinkUtil {
    public static void setConfig(String param, TableEnvironmentImpl impl) {
        if (StringUtils.isNotEmpty(param)) {
            for (String conf : param.split(";")) {
                impl.getConfig().getConfiguration().setString(conf.split("=")[0], conf.split("=")[1]);
            }
        }
    }

    public static FlinkJob getJobInofs(String url){
        FlinkJob job = null;
        try (CloseableHttpClient httpClient = HttpClients.createDefault()){
            HttpGet httpGet = new HttpGet(url);
            CloseableHttpResponse response = httpClient.execute(httpGet);
            String responseStr = EntityUtils.toString(response.getEntity(), "UTF-8");
            Gson gson = new Gson();
            job = gson.fromJson(responseStr, FlinkJob.class);
            return job;
        } catch (Exception e){
            log.error(FlinkUtil.class.getName() + " error ", e.getMessage());
        }
        return job;
    }

    public static void setTransformationUid(List<Transformation<?>> transformations, Map<String, Integer> operatorMap){
        for (Transformation<?> transformation : transformations){
            if (CollectionUtils.isNotEmpty(transformation.getInputs())){
                setTransformationUid(transformation.getInputs(), operatorMap);
            }
            String name = transformation.getName();
            if (operatorMap.containsKey(name)){
                int idx = operatorMap.get(name);
                operatorMap.put(name, ++idx);
                transformation.setUid(name + idx);
                transformation.setUidHash(OperatorIDGenerator.fromUid(name + idx).toHexString());
            } else {
                operatorMap.put(name, 0);
                transformation.setUid(transformation.getName());
                transformation.setUidHash(OperatorIDGenerator.fromUid(transformation.getName()).toHexString());
            }
        }
    }

    public static Map<Integer, Map<String, String>> getHintsFromString(String hints, int size){
        Map<Integer, Map<String, String>> map = new HashMap<>();
        if (StringUtils.isEmpty(hints)){
            return map;
        }

        String[] hintsConfig = hints.split(";", size);
        if (size != hintsConfig.length){
            throw new SqlGatewayException("hints configuration size not equals to job size");
        }

        for (int i = 0; i < size; i++){
            if (StringUtils.isNotEmpty(hintsConfig[i])){
                Map hint = new HashMap<String, String>();
                String[] oneHint = hintsConfig[i].split(",", 2);
                for (int k = 0; k < oneHint.length; k++){
                    hint.put(oneHint[k].split("=")[0].trim(), oneHint[k].split("=")[1].trim());
                }
                map.put(i, hint);
            }
        }

        return map;
    }

    private static SqlNode updateSqlTableRefByHints(SqlTableRef originalSqlTableRef, Map<String, String> hintsMap){
        if (MapUtils.isEmpty(hintsMap)){
            return originalSqlTableRef;
        }

        SqlIdentifier tableName = (SqlIdentifier) originalSqlTableRef.getOperandList().get(0);
        SqlNodeList hints = (SqlNodeList) originalSqlTableRef.getOperandList().get(1);
        if (CollectionUtils.isNotEmpty(hints.getList())) {
            Map<String, String> all = new HashMap();
            SqlHint sqlHint = (SqlHint) hints.get(0);
            all.putAll(sqlHint.getOptionKVPairs());
            all.putAll(hintsMap);
            SqlNodeList newOptions = new SqlNodeList(SqlParserPos.ZERO);
            all.forEach((k, v) -> {
                newOptions.add(SqlCharStringLiteral.createCharString(k, hints.getParserPosition()));
                newOptions.add(SqlCharStringLiteral.createCharString(v, hints.getParserPosition()));
            });
            SqlHint newSqlHint = new SqlHint(sqlHint.getParserPosition(),
                    new SqlIdentifier(sqlHint.getName(), sqlHint.getParserPosition()),
                    newOptions,
                    sqlHint.getOptionFormat());
            SqlTableRef newTableRef = new SqlTableRef(originalSqlTableRef.getParserPosition(),
                    tableName,
                    SqlNodeList.of(newSqlHint));
            return newTableRef;
        } else {
            SqlTableRef newTableRef = new SqlTableRef(originalSqlTableRef.getParserPosition(),
                    tableName,
                    SqlNodeList.of(createSqlHintByHints(hints, hintsMap)));
            return newTableRef;
        }
    }

    private static SqlHint createSqlHintByHints(SqlNode node, Map<String, String> hintsMap){
        SqlNodeList newOptions = new SqlNodeList(SqlParserPos.ZERO);
        hintsMap.forEach((k, v) -> {
            newOptions.add(SqlCharStringLiteral.createCharString(k, node.getParserPosition()));
            newOptions.add(SqlCharStringLiteral.createCharString(v, node.getParserPosition()));
        });
        SqlHint newSqlHint = new SqlHint(node.getParserPosition(),
                new SqlIdentifier("options", node.getParserPosition()),
                newOptions,
                SqlHint.HintOptionFormat.KV_LIST);
        return newSqlHint;
    }

    private static SqlNode updateSqlBasicCallSqlNodeByHints(SqlNode node, Map<String, String> hintsMap){
        if (node instanceof SqlSelect){
            return injectHints(node, hintsMap);
        }

        if (node instanceof SqlTableRef){
            return updateSqlTableRefByHints((SqlTableRef) node, hintsMap);
        }
        // ignore SqlIdentifier
//        if (node instanceof SqlIdentifier){
//            return updateSqlIdentifierByHints((SqlIdentifier)node, hintsMap);
//        }
        return node;
    }

    private static SqlNode updateSqlIdentifierByHints(SqlIdentifier sqlIdentifier, Map<String, String> hintsMap){
        SqlTableRef sqlTableRef = new SqlTableRef(sqlIdentifier.getParserPosition(),
                sqlIdentifier,
                SqlNodeList.of(createSqlHintByHints(sqlIdentifier,hintsMap)));
        return sqlTableRef;
    }

    private static SqlBasicCall updateSqlBasicCallByHints(SqlBasicCall originalSqlBasicCall, Map<String, String> hintsMap){
        SqlNode[] newOperands = {
                                    updateSqlBasicCallSqlNodeByHints(originalSqlBasicCall.getOperands()[0], hintsMap),
                                    updateSqlBasicCallSqlNodeByHints(originalSqlBasicCall.getOperands()[1], hintsMap)
                                };
        SqlBasicCall newSqlBasicCall = new SqlBasicCall(originalSqlBasicCall.getOperator(),
                newOperands,
                originalSqlBasicCall.getParserPosition());
        return newSqlBasicCall;
    }

    /**
     * Inject custom hints to a Sqlnode.
     * if duplicate options from node and hintsMap, the hintsMap option will overwrite node
     * @param node the SqlNode like SqlSelect, SqlJoin, SqlBasicCall.
     * @param hintsMap the options which SqlNode's SqlTableRef will be injected.
     * @return injected hintsMap SqlNode
     */
    public static SqlNode injectHints(SqlNode node, Map<String, String> hintsMap){
        if (node instanceof SqlTableRef) {
            SqlTableRef sqlTableRef = (SqlTableRef) node;
            SqlTableRef newTableRef = (SqlTableRef)updateSqlTableRefByHints(sqlTableRef, hintsMap);
            return newTableRef;
        }

        if (node instanceof SqlSelect) {
            SqlSelect select = (SqlSelect)node;
            SqlNode newSelect = injectHints(select.getFrom(), hintsMap);
            SqlSelect newSqlSelect = new SqlSelect(select.getParserPosition(),
                    (SqlNodeList) select.getOperandList().get(0),
                    (SqlNodeList) select.getOperandList().get(1),
                    newSelect,
                    select.getWhere(),
                    select.getGroup(),
                    select.getHaving(),
                    select.getWindowList(),
                    select.getOrderList(),
                    select.getOffset(),
                    select.getFetch(),
                    select.getHints());
            return newSqlSelect;
        }

        if (node instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall)node;
            SqlBasicCall newSqlBasicCall = updateSqlBasicCallByHints(sqlBasicCall, hintsMap);
            return newSqlBasicCall;
        }

        if (node instanceof SqlJoin){
            SqlJoin sqlJoin = (SqlJoin)node;
            SqlBasicCall left = (SqlBasicCall)sqlJoin.getLeft();
            SqlBasicCall right = (SqlBasicCall)sqlJoin.getRight();
            SqlJoin newSqlJoin = new SqlJoin(sqlJoin.getParserPosition(),
                    updateSqlBasicCallByHints(left, hintsMap),
                    (SqlLiteral)sqlJoin.getOperandList().get(1),
                    (SqlLiteral)sqlJoin.getOperandList().get(2),
                    updateSqlBasicCallByHints(right, hintsMap),
                    (SqlLiteral)sqlJoin.getOperandList().get(4),
                    sqlJoin.getOperandList().get(5)
                    );
            return newSqlJoin;
        }

        return node;
    }
}
