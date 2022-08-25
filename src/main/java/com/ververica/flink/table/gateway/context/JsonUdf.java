package com.ververica.flink.table.gateway.context;

import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.flink.table.functions.ScalarFunction;

public class JsonUdf {
    public static class extractFromJson extends ScalarFunction{

        public String eval(String json, String key){
            JSONObject object = JSONObject.parseObject(json);
            String result = "";
            try {
                result = object.getString(key);
            }catch (Exception ignored){
            }
            return result;
        }
    }

    public static class debeziumJsonTrans extends ScalarFunction{

        public String eval(String json){
            JSONObject object = JSONObject.parseObject(json);
            try {
                JSONObject dataOjb = object.getJSONObject("after");
                dataOjb.put("_extData", object.getJSONObject("source"));
                return JSONObject.toJSONString(dataOjb);
            }catch (Exception ignored){
            }
            return object.toString();
        }
    }

    public static class addDbTableToPrimaryKey extends ScalarFunction{

        @SneakyThrows
        public String eval(String json, String primaryKeyColumn, Integer dbNoLastIdx, Integer tableNoLastIdx){
            if(null == dbNoLastIdx && null == tableNoLastIdx){
                return json;
            }
            JSONObject object = JSONObject.parseObject(json);
            try {
                JSONObject sourceObj = object.getJSONObject("source");
                String db = sourceObj.getString("db");
                String table = sourceObj.getString("table");
                String suffix = "";
                if(null != dbNoLastIdx){
                    String lastDb = db.substring(db.length() - dbNoLastIdx);
                    suffix = suffix + lastDb;
                }
                if(null != tableNoLastIdx){
                    String lastTable = table.substring(table.length() - tableNoLastIdx);
                    suffix = suffix + lastTable;
                }
                JSONObject after = object.getJSONObject("after");
                String idStr = after.getString(primaryKeyColumn);
                idStr = idStr + suffix;
                after.put(primaryKeyColumn, Long.valueOf(idStr));
                object.put("after", after);
                return JSONObject.toJSONString(object);
            }catch (Exception exception){
                exception.printStackTrace();
            }
            return JSONObject.toJSONString(object);
        }
    }

    public static class extractFromJsonPath extends ScalarFunction{

        public String eval(String json, String path){
            JSONObject object = JSONObject.parseObject(json);
            int i = 0;
            int length = path.split("\\.").length;
            for(String p:path.split("\\.")){
                i++;
                if(i < length){
                    object = object.getJSONObject(p);
                }else{
                    return object.getString(p);
                }
            }
            return null;
        }
    }
}
