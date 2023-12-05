package com.ververica.flink.table.gateway.rest.message;

import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * {@link RequestBody} for Job Metric.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobMetricRequestBody implements RequestBody{

    private static final String URL = "url";

    @JsonProperty(URL)
    private final String url;

    public JobMetricRequestBody(@JsonProperty(URL) String url) {
        this.url = url;
    }

    public String getUrl(){
        return this.url;
    }
}
