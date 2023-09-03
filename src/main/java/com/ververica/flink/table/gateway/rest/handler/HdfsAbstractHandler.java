package com.ververica.flink.table.gateway.rest.handler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;

/**
 * Request abstract handler for hdfs file.
 */
public abstract class HdfsAbstractHandler <R extends RequestBody, P extends ResponseBody>
        extends AbstractRestHandler<R, P, EmptyMessageParameters> {
    protected FileSystem fs;
    protected Configuration conf;

    protected HdfsAbstractHandler(Time timeout, Map<String, String> responseHeaders,
                                  MessageHeaders<R, P, EmptyMessageParameters> messageHeaders) {
        super(timeout, responseHeaders, messageHeaders);
        try {
            conf = new Configuration();
            String dir = System.getProperty("yarn.conf.dir");
            conf.addResource(new Path(dir + "/yarn-site.xml"));
            conf.addResource(new Path(dir + "/core-site.xml"));
            conf.addResource(new Path(dir + "/hdfs-site.xml"));
            conf.setBoolean("fs.hdfs.impl.disable.cache", true);
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
