package com.ververica.flink.table.gateway.rest.entity;

import com.ververica.flink.table.gateway.rest.result.ResultSet;
import lombok.Data;

/**
 * YarnJobInfoResult for {@link ResultSet}.
 */
@Data
public class YarnJobInfoResult {
    private String appId;
    private String jobName;
    private String trackingUrl;
    private FlinkJob flinkJob;
    private String yarnState;
    private long yarnStartTime;
}
