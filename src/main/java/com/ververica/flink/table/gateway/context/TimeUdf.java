package com.ververica.flink.table.gateway.context;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


public class TimeUdf {
    @Deprecated
    public static class format_time_yyyy_MM_dd_function extends ScalarFunction {
        public String eval(Long value){
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(value);
            return sdf.format(c.getTime());
        }
    }

    public static class format_time_function extends ScalarFunction {
        public String eval(String value){
            Format format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            Calendar c = Calendar.getInstance();
            try {
                Date date = new Date(Long.valueOf(value));
                c.setTime(date);
                c.set(Calendar.MINUTE, (c.get(Calendar.MINUTE)/5 *5) + 5);
                c.set(Calendar.SECOND, 0);
                c.set(Calendar.MILLISECOND, 0);
                return format.format(c.getTime());
            }catch (Exception e){
                e.printStackTrace();
            }
            return format.format(c.getTime());
        }
    }

    public static class unixEpochToTimestamp extends ScalarFunction{
        public Timestamp eval(String value){
            try {
                return new Timestamp(Long.valueOf(value));
            }catch (Exception e){
            }
            return new Timestamp(0);
        }
    }

//    public static class unixEpochToTimestamp3 extends ScalarFunction{
//        public @DataTypeHint("TIMESTAMP(3)") Timestamp eval(String value){
//            try {
//                return new Timestamp(Long.valueOf(value));
//            }catch (Exception e){
//            }
//            return new Timestamp(0);
//        }
//    }

    public static class parseTimeDiff extends ScalarFunction{
        public String eval(Integer value){
            try {
                Calendar c = Calendar.getInstance();
                c.set(Calendar.YEAR, 1970);
                c.set(Calendar.MONTH, 1);
                c.set(Calendar.DAY_OF_YEAR, 1);
                c.add(Calendar.DATE, value);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                return sdf.format(c.getTime());
            }catch (Exception e){
                e.printStackTrace();
            }
            return "";
        }
    }

    public static class isTimestampCurrentDay extends ScalarFunction{
        public Boolean eval(Long value){
            try {
                Long now = System.currentTimeMillis();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                return sdf.format(now).equalsIgnoreCase(sdf.format(value));
            }catch (Exception e){
                e.printStackTrace();
            }
            return false;
        }
    }

    public static class isTimeDelay extends ScalarFunction{
        public boolean eval(Long time, Integer minutes){
            Long now = System.currentTimeMillis();
            if(time + 60*1000*minutes < now){
                return true;
            }
            return false;
        }
    }

    @Deprecated
    public static class formatTime extends ScalarFunction{
        public String eval(Long value, String formatter){
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(formatter);
                return sdf.format(new Date(value));
            }catch (Exception e){
                e.printStackTrace();
            }
            return "";
        }
    }

    public static class formatTimestamp extends ScalarFunction{
        public Long eval(String value, String formatter){
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(formatter);
                return sdf.parse(value).getTime();
            }catch (Exception e){
                e.printStackTrace();
            }
            return 0L;
        }
    }

}
