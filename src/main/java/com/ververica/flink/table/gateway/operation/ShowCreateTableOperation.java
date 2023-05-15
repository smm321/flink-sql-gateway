package com.ververica.flink.table.gateway.operation;

import com.ververica.flink.table.gateway.context.ExecutionContext;
import com.ververica.flink.table.gateway.context.SessionContext;
import com.ververica.flink.table.gateway.rest.result.ColumnInfo;
import com.ververica.flink.table.gateway.rest.result.ConstantNames;
import com.ververica.flink.table.gateway.rest.result.ResultKind;
import com.ververica.flink.table.gateway.rest.result.ResultSet;
import com.ververica.flink.table.gateway.utils.SqlExecutionException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Operation for SHOW CREATE TABLE command.
 * include all information of a table except like_options
 * yonghui.lu
 */
public class ShowCreateTableOperation implements NonJobOperation{

    private final ExecutionContext<?> context;
    private final String tableName;

    public ShowCreateTableOperation(SessionContext context, String tableName) {
        this.context = context.getExecutionContext();
        this.tableName = tableName;
    }

    @Override
    public ResultSet execute() {
        TableEnvironment tableEnv = context.getTableEnvironment();
        Catalog catalog = tableEnv.getCatalog(tableEnv.getCurrentCatalog())
                .orElseThrow(
                        //impossible to be here
                        () -> new SqlExecutionException("No catalog with this name : " +
                                tableEnv.getCurrentCatalog() + " could be found.")
                );
        try {
            ResolvedSchema schema = context.wrapClassLoader(() -> tableEnv.from(tableName).getResolvedSchema());
            //----------------------------------------------------------------------------------------------------------
            // Step.1 get column info
            //----------------------------------------------------------------------------------------------------------
            List<Column> fieldInfo = schema.getColumns();
            //----------------------------------------------------------------------------------------------------------
            // Step.2 get watermark
            //----------------------------------------------------------------------------------------------------------
            Map<String, WatermarkSpec> watermark = schema
                    .getWatermarkSpecs()
                    .stream()
                    .collect(Collectors.toMap(WatermarkSpec::getRowtimeAttribute, Function.identity()));
            //----------------------------------------------------------------------------------------------------------
            // Step.3 get primary keys with String format
            //----------------------------------------------------------------------------------------------------------
            StringBuffer primaryKey = new StringBuffer();
            schema.getPrimaryKey().ifPresent(o -> {
                String typeString;
                switch(o.getType()) {
                    case PRIMARY_KEY:
                        typeString = "PRIMARY KEY";
                        break;
                    case UNIQUE_KEY:
                        typeString = "UNIQUE";
                        break;
                    default:
                        throw new IllegalStateException("Unknown key type: " + o.getType());
                }
                primaryKey.append(String.format("%s (%s)%s", typeString,
                        o.getColumns().stream().map(EncodingUtils::escapeIdentifier)
                                .collect(Collectors.joining(", ")),
                        o.isEnforced() ? "" : " NOT ENFORCED"));
            });
            //----------------------------------------------------------------------------------------------------------
            // Step.4 get table comment with String format
            //----------------------------------------------------------------------------------------------------------
            CatalogBaseTable catalogBaseTable =
                    catalog.getTable(new ObjectPath(tableEnv.getCurrentDatabase(), tableName));
            String comment = catalogBaseTable.getComment();
            //----------------------------------------------------------------------------------------------------------
            // Step.5 get partition column
            //----------------------------------------------------------------------------------------------------------
            List<String> partitionList = new ArrayList<>();
            if (catalog instanceof HiveCatalog){
                HiveCatalog hiveCatalog = (HiveCatalog)catalog;
                hiveCatalog.getHiveTable(new ObjectPath(tableEnv.getCurrentDatabase(), tableName)).getParameters()
                        .forEach((k, v) -> {
                            if (k.startsWith("flink.partition.keys")){
                                partitionList.add(v);
                            }
                        });
            }
            //----------------------------------------------------------------------------------------------------------
            // Step.6 get with info
            //----------------------------------------------------------------------------------------------------------
            Map<String, String> withMap = catalogBaseTable.getOptions();

            return ResultSet.builder()
                    .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
                    .columns(ColumnInfo.create(ConstantNames.DQL, new VarCharType()))
                    .data(Row.of(generateCreateTableDDL(fieldInfo, watermark, primaryKey.toString(), comment,
                            partitionList, withMap, tableName)))
                    .build();
        } catch (Exception e){
            e.printStackTrace();
            throw new SqlExecutionException(String.format("show create table : %s failed, Caused by %s",
                    tableName, e.getMessage()));
        }
    }

    private String generateCreateTableDDL(List<Column> fieldInfo, Map<String, WatermarkSpec> watermark,
        String primaryKey, String comment, List<String> partitionList, Map<String, String> with, String tableName){
        StringBuffer stringBuffer = new StringBuffer("CREATE TABLE ")
                .append(EncodingUtils.escapeIdentifier(tableName.trim()))
                .append("(");
        fieldInfo.forEach((v) -> {
            if (v instanceof Column.PhysicalColumn){
                Column.PhysicalColumn col = (Column.PhysicalColumn)v;
                String colType = col.getDataType().getLogicalType().toString();
                if (watermark.containsKey(v.getName())){
                    colType = StringUtils.removeEnd(colType, " *ROWTIME*");
                } else {
                    colType = StringUtils.removeEnd(colType, " NOT NULL");
                }
                stringBuffer.append(EncodingUtils.escapeIdentifier(v.getName()))
                        .append(" ").append(colType).append(", ");
            }
            else if (v instanceof Column.ComputedColumn){
                //we can't use ComputedColumn.toString() here
                Column.ComputedColumn col = (Column.ComputedColumn)v;
                String exp = "AS " + col.getExpression();
                stringBuffer.append(EncodingUtils.escapeIdentifier(v.getName())).append(" ").append(exp).append(", ");
            } else if (v instanceof Column.MetadataColumn){
                //we can't use Column.MetadataColumn.toString() here
                Column.MetadataColumn col = (Column.MetadataColumn)v;
                String type = col.getDataType().getLogicalType().toString();
                if (watermark.containsKey(v.getName())){
                    type = StringUtils.removeEnd(type, " *ROWTIME*");
                }
                stringBuffer.append(EncodingUtils.escapeIdentifier(v.getName())).append(" ").append(type);
                //add meta info
                col.explainExtras().ifPresent((e) -> {
                    stringBuffer.append(" ");
                    stringBuffer.append(e);
                });
                stringBuffer.append(", ");
            }
        });
        if (StringUtils.isNotEmpty(primaryKey)){
            stringBuffer.append(primaryKey).append(", ");
        }
        watermark.forEach((k, v) -> {
            String str = " WATERMARK FOR " + String.join(".",
                    EncodingUtils.escapeIdentifier(v.getRowtimeAttribute()))
                    + " AS " + v.getWatermarkExpression().asSummaryString();
            stringBuffer.append(str).append(", ");
        });
        stringBuffer.deleteCharAt(stringBuffer.length() - 2).deleteCharAt(stringBuffer.length() - 1).append(")");
        if (StringUtils.isNotEmpty(comment)){
            stringBuffer.append(" COMMENT '").append(comment).append("'");
        }
        if (CollectionUtils.isNotEmpty(partitionList)){
            stringBuffer.append(" PARTITIONED BY (").append(String.join(", ", partitionList)).append(")");
        }
        if (!with.isEmpty()){
            stringBuffer.append(" WITH(");
            SortedSet<String> keys = new TreeSet<>(with.keySet());
            for (String key : keys) {
                String value = with.get(key);
                stringBuffer.append("'").append(key).append("'='").append(value).append("', ");
            }
            stringBuffer.deleteCharAt(stringBuffer.length() - 2).deleteCharAt(stringBuffer.length() - 1).append(")");
        }
        return stringBuffer.toString();
    }
}
