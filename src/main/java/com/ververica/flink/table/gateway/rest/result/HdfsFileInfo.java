package com.ververica.flink.table.gateway.rest.result;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.util.Preconditions;

/**
 * hdfs file result.
 */
public class HdfsFileInfo {

    private static final String FIELD_NAME_FILE_NAME = "fileName";
    private static final String FIELD_NAME_FILE_TIME = "fileTime";


    @JsonProperty(FIELD_NAME_FILE_NAME)
    private String fileName;

    @JsonProperty(FIELD_NAME_FILE_TIME)
    private String fileTime;

    @JsonCreator
    public HdfsFileInfo(
            @JsonProperty(FIELD_NAME_FILE_NAME) String fileName,
            @JsonProperty(FIELD_NAME_FILE_TIME) String fileTime) {
        this.fileName = Preconditions.checkNotNull(fileName, "fileName must not be null");
        this.fileTime = Preconditions.checkNotNull(fileTime, "fileTime must not be null");
    }

    public String getFileName() {
        return fileName;
    }

    public String getFileTime() {
        return fileTime;
    }

}
