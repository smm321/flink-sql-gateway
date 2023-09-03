package com.ververica.flink.table.gateway.rest.entity;

import com.ververica.flink.table.gateway.rest.result.ResultSet;
import lombok.Data;

/**
 * SavepointResult for {@link ResultSet}.
 */
@Data
public class SavepointResult {
    private String appId;
    private String jobName;
    private String jobId;
    private String savepointUrl;
}
