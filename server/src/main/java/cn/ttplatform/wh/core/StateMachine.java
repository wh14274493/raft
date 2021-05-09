package cn.ttplatform.wh.core;

import cn.ttplatform.wh.constant.ErrorMessage;
import cn.ttplatform.wh.support.PooledByteBuffer;
import cn.ttplatform.wh.exception.MessageParseException;
import cn.ttplatform.wh.support.Factory;
import cn.ttplatform.wh.support.Pool;
import io.protostuff.ByteBufferInput;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/21 16:16
 */
@Slf4j
public class StateMachine {

    private Data data = new Data();
    private final DataFactory dataFactory;
    private int lastApplied;


    public StateMachine(GlobalContext context) {
        this.dataFactory = new DataFactory(context.getLinkedBufferPool());
    }

    public String get(String key) {
        return data.get(key);
    }

    public void set(String key, String value) {
        data.put(key, value);
    }

    public int getLastApplied() {
        return lastApplied;
    }

    public void setLastApplied(int lastApplied) {
        this.lastApplied = lastApplied;
    }

    public byte[] generateSnapshotData() {
        return dataFactory.getBytes(data);
    }

    public void applySnapshotData(PooledByteBuffer snapshot, int lastIncludeIndex) {
        data = dataFactory.create(snapshot.getBuffer(), snapshot.limit());
        lastApplied = lastIncludeIndex;
        log.info("apply snapshot that lastIncludeIndex is {}.", lastIncludeIndex);
        snapshot.recycle();
    }

    private static class Data extends HashMap<String, String> {

    }

    private static class DataFactory implements Factory<Data> {

        private static final Schema<Data> DATA_SCHEMA = RuntimeSchema.getSchema(Data.class);
        private final Pool<LinkedBuffer> pool;

        public DataFactory(Pool<LinkedBuffer> pool) {
            this.pool = pool;
        }

        @Override
        public Data create(byte[] content, int length) {
            Data data = new Data();
            ProtostuffIOUtil.mergeFrom(content, 0, length, data, DATA_SCHEMA);
            return data;
        }

        @Override
        public Data create(ByteBuffer byteBuffer, int contentLength) {
            int limit = byteBuffer.limit();
            try {
                int position = byteBuffer.position();
                byteBuffer.limit(position + contentLength);
                Data data = new Data();
                try {
                    DATA_SCHEMA.mergeFrom(new ByteBufferInput(byteBuffer, true), data);
                } catch (IOException e) {
                    throw new MessageParseException(ErrorMessage.MESSAGE_PARSE_ERROR);
                }
                return data;
            } finally {
                byteBuffer.limit(limit);
            }
        }

        @Override
        public byte[] getBytes(Data data) {
            LinkedBuffer buffer = pool.allocate();
            try {
                return ProtostuffIOUtil.toByteArray(data, DATA_SCHEMA, buffer);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            } finally {
                pool.recycle(buffer);
            }
        }
    }

}
