package com.ververica.flink.table.gateway.utils;

import com.google.gson.Gson;
import com.ververica.flink.table.gateway.rest.entity.FlinkJob;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.util.Arrays;
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

//    public static SqlNodeList unionHints(Map<String, String> map, SqlNodeList hints){
//        Map<String, String> finalPairs = new HashMap<>();
//        for (SqlNode h : hints){
//            SqlHint hint = (SqlHint)h;
////            String name = hint.getName();
////        SqlIdentifier.clone(hint.)
////            SqlIdentifier identifier = new SqlIdentifier(name, hint.getParserPosition());
//            finalPairs.putAll(hint.getOptionKVPairs());
////            SqlHint newHint = new SqlHint(hint.getParserPosition(), identifier,null, hint.getOptionFormat());
//        }
//        finalPairs.putAll(map);
//        finalPairs.forEach((k, v) -> {
////            SqlHint hint = new SqlHint()
////        });
//        return null;
//    }

    public static SqlNode injectHints(SqlNode node, Map<String, String> hintsMap){
        if (node instanceof SqlSelect){
            SqlSelect select = (SqlSelect)node;
            if (select.getFrom() instanceof SqlTableRef){
                SqlTableRef sqlTableRef = (SqlTableRef)select.getFrom();
                SqlIdentifier tableName = (SqlIdentifier)sqlTableRef.getOperandList().get(0);
                SqlNodeList hints = (SqlNodeList)sqlTableRef.getOperandList().get(1);
                if(CollectionUtils.isNotEmpty(hints.getList())){
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
                            new SqlIdentifier(sqlHint.getName(),
                            sqlHint.getParserPosition()),
                            newOptions,
                            sqlHint.getOptionFormat());
                    SqlTableRef newTableRef = new SqlTableRef(sqlTableRef.getParserPosition(),
                            tableName,
                            SqlNodeList.of(newSqlHint));
                    SqlSelect newSqlSelect = new SqlSelect(select.getParserPosition(),
                            (SqlNodeList)select.getOperandList().get(0),
                            (SqlNodeList)select.getOperandList().get(1),
                            newTableRef,
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

                SqlNodeList newOptions = new SqlNodeList(SqlParserPos.ZERO);
                hintsMap.forEach((k, v) -> {
                    newOptions.add(SqlCharStringLiteral.createCharString(k, hints.getParserPosition()));
                    newOptions.add(SqlCharStringLiteral.createCharString(v, hints.getParserPosition()));
                });
                SqlHint newSqlHint = new SqlHint(sqlTableRef.getParserPosition(),
                        new SqlIdentifier("options",
                        sqlTableRef.getParserPosition()),
                        newOptions,
                        SqlHint.HintOptionFormat.KV_LIST);
                SqlTableRef newTableRef = new SqlTableRef(sqlTableRef.getParserPosition(),
                        tableName,
                        SqlNodeList.of(newSqlHint));
                SqlSelect newSqlSelect = new SqlSelect(select.getParserPosition(),
                        (SqlNodeList)select.getOperandList().get(0),
                        (SqlNodeList)select.getOperandList().get(1),
                        newTableRef,
                        select.getWhere(),
                        select.getGroup(),
                        select.getHaving(),
                        select.getWindowList(),
                        select.getOrderList(),
                        select.getOffset(),
                        select.getFetch(),
                        select.getHints());
                return newSqlSelect;
            } else {
                if (select.getFrom() instanceof SqlSelect){
                    SqlSelect newTableRef = (SqlSelect)injectHints(select.getFrom(), hintsMap);
                    SqlSelect newSqlSelect = new SqlSelect(select.getParserPosition(),
                            (SqlNodeList)select.getOperandList().get(0),
                            (SqlNodeList)select.getOperandList().get(1),
                            newTableRef,
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
                if (select.getFrom() instanceof SqlBasicCall){

                }
            }
        }
        if (node instanceof SqlJoin){
            SqlJoin sqlJoin = (SqlJoin)node;
            SqlBasicCall left = (SqlBasicCall)sqlJoin.getLeft();
            SqlBasicCall right = (SqlBasicCall)sqlJoin.getRight();
            SqlIdentifier table = (SqlIdentifier)Arrays.stream(left.getOperands()).findFirst().get();
        }
//        SqlSelect select = (SqlSelect)from;
//        if (null != select.getFrom()){
//            return getSqlTableRefsFromSqlSelect()
//        }
        return node;
    }
}
