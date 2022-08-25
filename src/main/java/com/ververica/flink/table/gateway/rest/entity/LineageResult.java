package com.ververica.flink.table.gateway.rest.entity;

import com.ververica.flink.table.gateway.rest.result.ResultSet;
import lombok.Data;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * LineageResult for {@link ResultSet}.
 */
@Data
public class LineageResult {
    private String sinkDbName;
    private String sinkTableName;
    private List<Tuple2<String, String>> sourceTable = new ArrayList<>();
    private List<Map<String, Tuple3<String, String, String>>> sourceColumn = new ArrayList<>();
}
