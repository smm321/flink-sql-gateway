package com.ververica.flink.table.gateway.rest.handler;

import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import com.ververica.flink.table.gateway.rest.message.StateParserRequestBody;
import com.ververica.flink.table.gateway.rest.message.StateParserResponseBody;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.readBinlogPosition;
import static com.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.serializedStringToRow;

/**
 * Handler for getting a job's info.
 */
public class StateParserHandler extends AbstractRestHandler<StateParserRequestBody, StateParserResponseBody,
        EmptyMessageParameters> {

    public StateParserHandler(Time timeout, Map responseHeaders,
                              MessageHeaders<StateParserRequestBody,
                              StateParserResponseBody, EmptyMessageParameters> messageHeaders) {
        super(timeout, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture handleRequest(@Nonnull HandlerRequest<StateParserRequestBody> request) throws RestHandlerException {
        List<List<String>> response = new ArrayList<>();
        try {
            String url = request.getRequestBody().getStateUrl();
            String type = request.getRequestBody().getStateType();
            String uid = request.getRequestBody().getStateUid();
            ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
            ExistingSavepoint sp = Savepoint.load(environment, url, new HashMapStateBackend());
            if ("cdc".equalsIgnoreCase(type)) {
                // old stream API with prev cdc connector
//                DataSet<String> historyDs = sp.readUnionState(uid,"history-records-states", Types.STRING(), StringSerializer.INSTANCE);
//                DataSet offsetDs = sp.readUnionState(uid, "offset-states",
//                        PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
//                        new BytePrimitiveArraySerializer());
//                response.add(0, historyDs.collect());
//                response.add(1, offsetDs.map(o -> new String((byte[])o, StandardCharsets.UTF_8)).collect());
                DataSet<byte[]> mysqlSplitState = sp.readListState(uid,"SourceReaderState",
                        PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
                DataSet<MySqlSplit> splits = mysqlSplitState.map(o -> {
                    DataInputDeserializer in = new DataInputDeserializer(o);
                    in.skipBytes(8);
                    int version = in.readInt();
                    String splitId = in.readUTF();
                    // skip split Key Type
                    in.readUTF();
                    BinlogOffset startingOffset = readBinlogPosition(version, in);
                    BinlogOffset endingOffset = readBinlogPosition(version, in);
                    List<FinishedSnapshotSplitInfo> finishedSplitsInfo =
                            readFinishedSplitsInfo(version, in);
                    Map<TableId, TableChanges.TableChange> tableChangeMap = readTableSchemas(version, in);
                    int totalFinishedSplitSize = finishedSplitsInfo.size();
                    boolean isSuspended = false;
                    if (version >= 3) {
                        totalFinishedSplitSize = in.readInt();
                        if (version > 3) {
                            isSuspended = in.readBoolean();
                        }
                    }
                    in.releaseArrays();
                    return (MySqlSplit) new MySqlBinlogSplit(
                            splitId,
                            startingOffset,
                            endingOffset,
                            finishedSplitsInfo,
                            tableChangeMap,
                            totalFinishedSplitSize,
                            isSuspended);
                });
                response.add(splits.map(o -> {
                    MySqlBinlogSplit curSplit = o.asBinlogSplit();
                    ObjectMapper parser = new ObjectMapper();
                    ObjectNode ret = parser.createObjectNode();
                    ret.put("type", curSplit.splitId());
                    ret.put("offsets", parser.valueToTree(curSplit.getStartingOffset()));
                    ret.put("schema", parser.valueToTree(curSplit.getTableSchemas()));
                    return ret.toString();
                    // todo: to implement sourceStateResult entity to get valid mysqlSplitState
//                    return SourceStateResult.builder()
//                            .tableSchemas(curSplit.getTableSchemas())
//                            .offsets(curSplit.getStartingOffset())
//                            .build();
                    }).collect());
                // todo : write to new savepoint
//                MySqlSplitSerializer splitSerializer = MySqlSplitSerializer.INSTANCE;
//                BootstrapTransformation transformation = OperatorTransformation
//                        .bootstrapWith(splits)
//                        .transform(new SimpleStateBootstrapFunction(splitSerializer));
//                sp.withOperator("uid", transformation).write("/tmp/write_cdc_chk");
            } else {
                TypeHint typeHinthint = new TypeHint<Tuple2<KafkaTopicPartition, Long>>(){};
                DataSet offsetDs = sp.readUnionState(uid,
                        "topic-partition-offset-states",
                        TypeInformation.of(typeHinthint),
                        createStateSerializer(new ExecutionConfig()));
                response.add(0, offsetDs.collect());
//useless
//                TypeHint typeHint1 = new TypeHint<FlinkKafkaProducer011.NextTransactionalIdHint>(){};
//                DataSet txnDs = sp.readUnionState(uid,
//                        "next-transactional-id-hint-v2",
//                        TypeInformation.of(typeHint1),
//                        new FlinkKafkaProducer.NextTransactionalIdHintSerializer());
//                response.add(1, txnDs.collect());
            }
        } catch (Exception e) {
            throw new RestHandlerException(e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
        }
        return CompletableFuture.completedFuture(new StateParserResponseBody<>(response));
    }

    static TupleSerializer<Tuple2<KafkaTopicPartition, Long>> createStateSerializer(
            ExecutionConfig executionConfig) {
        // explicit serializer will keep the compatibility with GenericTypeInformation and allow to
        // disableGenericTypes for users
        TypeSerializer<?>[] fieldSerializers =
                new TypeSerializer<?>[] {
                        new KryoSerializer<>(KafkaTopicPartition.class, executionConfig),
                        LongSerializer.INSTANCE
                };
        @SuppressWarnings("unchecked")
        Class<Tuple2<KafkaTopicPartition, Long>> tupleClass =
                (Class<Tuple2<KafkaTopicPartition, Long>>) (Class<?>) Tuple2.class;
        return new TupleSerializer<>(tupleClass, fieldSerializers);
    }

    private static Map<TableId, TableChanges.TableChange> readTableSchemas(int version, DataInputDeserializer in)
            throws IOException {
        DocumentReader documentReader = DocumentReader.defaultReader();
        Map<TableId, TableChanges.TableChange> tableSchemas = new HashMap();
        int size = in.readInt();

        for (int i = 0; i < size; ++i) {
            TableId tableId = TableId.parse(in.readUTF());
            String tableChangeStr;
            switch(version) {
                case 1:
                    tableChangeStr = in.readUTF();
                    break;
                case 2:
                case 3:
                case 4:
                    int len = in.readInt();
                    byte[] bytes = new byte[len];
                    in.read(bytes);
                    tableChangeStr = new String(bytes, StandardCharsets.UTF_8);
                    break;
                default:
                    throw new IOException("Unknown version: " + version);
            }

            Document document = documentReader.read(tableChangeStr);
            TableChanges.TableChange tableChange = FlinkJsonTableChangeSerializer.fromDocument(document, true);
            tableSchemas.put(tableId, tableChange);
        }

        return tableSchemas;
    }

    private static List<FinishedSnapshotSplitInfo> readFinishedSplitsInfo(
            int version, DataInputDeserializer in) throws IOException {
        List<FinishedSnapshotSplitInfo> finishedSplitsInfo = new ArrayList<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            TableId tableId = TableId.parse(in.readUTF());
            String splitId = in.readUTF();
            Object[] splitStart = serializedStringToRow(in.readUTF());
            Object[] splitEnd = serializedStringToRow(in.readUTF());
            BinlogOffset highWatermark = readBinlogPosition(version, in);
            finishedSplitsInfo.add(
                    new FinishedSnapshotSplitInfo(
                            tableId, splitId, splitStart, splitEnd, highWatermark));
        }
        return finishedSplitsInfo;
    }
}


