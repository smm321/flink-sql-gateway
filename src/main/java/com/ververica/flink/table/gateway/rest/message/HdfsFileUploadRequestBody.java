package com.ververica.flink.table.gateway.rest.message;

import lombok.Data;
import org.apache.flink.runtime.rest.messages.RequestBody;
import javax.annotation.Nullable;
import java.io.File;

/**
 * HdfsFileUploadRequestBody.
 */
@Data
public class HdfsFileUploadRequestBody implements RequestBody {
    private static final String FIELD_NAME_PATH = "path";

    private final String path;

    private File file;

    public HdfsFileUploadRequestBody(String path) {
        this.path = path;
    }

    @Nullable
    public String getPath() {
        return path;
    }

    @Nullable
    public File getFile() {
        return file;
    }
}
