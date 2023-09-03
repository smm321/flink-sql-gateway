package com.ververica.flink.table.gateway.rest.message;

import lombok.Data;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;

/**
 * HdfsFileRequestBody.
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HdfsFileRequestBody implements RequestBody {
    private static final String FIELD_NAME_PATH = "path";

    @JsonProperty(FIELD_NAME_PATH)
    @Nullable
    private final String path;

    public HdfsFileRequestBody(@JsonProperty(FIELD_NAME_PATH) String path) {
        this.path = path;
    }

    @Nullable
    public String getPath() {
        return path;
    }
}
