package com.ververica.flink.table.gateway.rest.message;

import com.ververica.flink.table.gateway.rest.result.HdfsFileInfo;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 *HdfsFileResponseBody.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HdfsFileResponseBody implements ResponseBody {

    private static final String FIELD_NAME_RESULT = "results";

    @JsonProperty(FIELD_NAME_RESULT)
    private final List<HdfsFileInfo> results;

    public HdfsFileResponseBody(
            @JsonProperty(FIELD_NAME_RESULT) List<HdfsFileInfo> results) {
        this.results = results;
    }

    public List<HdfsFileInfo> getResults() {
        return results;
    }

}
