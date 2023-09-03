package com.ververica.flink.table.gateway.rest.message;

import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import javax.ws.rs.core.Response;

/**
 * HdfsFileDownloadResponseBody.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HdfsFileDownloadResponseBody implements ResponseBody {

    private Response response;

    public HdfsFileDownloadResponseBody(Response response) {
        this.response = response;
    }

}
