package com.ververica.flink.table.gateway.sink;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;

public class CollectDynamicStreamTableSink implements DynamicTableSink {

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return null;
    }

    @Override
    public DynamicTableSink copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}
