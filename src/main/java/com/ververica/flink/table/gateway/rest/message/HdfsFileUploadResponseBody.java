package com.ververica.flink.table.gateway.rest.message;

import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;

import javax.ws.rs.core.Response;

/**
 * HdfsFileUploadResponseBody.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HdfsFileUploadResponseBody implements ResponseBody {

    private Response response;

    public HdfsFileUploadResponseBody(Response response) {
        this.response = response;
    }

}
