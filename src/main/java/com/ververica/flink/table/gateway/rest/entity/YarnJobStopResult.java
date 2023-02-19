package com.ververica.flink.table.gateway.rest.entity;

import com.ververica.flink.table.gateway.rest.result.ResultSet;
import lombok.Data;

/**
 * YarnJobStopResult for {@link ResultSet}.
 */
@Data
public class YarnJobStopResult {
    private String appId;
    private String jobName;
    private String jobId;
    private String savepointUrl;
}
