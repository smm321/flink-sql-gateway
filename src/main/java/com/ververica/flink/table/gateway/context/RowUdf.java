package com.ververica.flink.table.gateway.context;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;


public class RowUdf {
    public static class rowToJson extends ScalarFunction {
        public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... row) throws Exception {
            if (row.length % 2 != 0) {
                throw new Exception("Wrong key/value pairs!");
            }

            Map map = new HashMap();
            IntStream.range(0, row.length).filter(index -> index % 2 == 0).forEach(index -> {
                String name = row[index].toString();
                Object value = row[index+1];
                map.put(name, value);
            });
            return JSONObject.toJSONString(map);
        }
    }

}
