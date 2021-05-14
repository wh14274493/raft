package cn.ttplatform.wh.core;

import cn.ttplatform.wh.constant.ErrorMessage;
import cn.ttplatform.wh.exception.MessageParseException;
import cn.ttplatform.wh.support.Factory;
import cn.ttplatform.wh.support.Pool;
import cn.ttplatform.wh.core.log.tool.PooledByteBuffer;
import io.netty.buffer.ByteBuf;
import io.protostuff.ByteBufferInput;
import io.protostuff.LinkedBuffer;
import io.protostuff.MessageMapSchema;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/21 16:16
 */
@Slf4j
public class StateMachine {

    private Map<String, String> data = new HashMap<>();
    private final DataFactory dataFactory;
    private int lastApplied;


    public StateMachine(GlobalContext context) {
        this.dataFactory = new DataFactory(context.getLinkedBufferPool());
    }

    public StateMachine(Pool<LinkedBuffer> pool) {
        this.dataFactory = new DataFactory(pool);
    }

    public String get(String key) {
        return data.get(key);
    }

    public void set(String key, String value) {
        data.put(key, value);
    }

    public int getPairs() {
        return data.size();
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

    private static class DataFactory implements Factory<Map<String, String>> {

        private final MessageMapSchema<String, String> mapSchema;
        private final Pool<LinkedBuffer> pool;

        public DataFactory(Pool<LinkedBuffer> pool) {
            Schema<String> stringSchema = RuntimeSchema.getSchema(String.class);
            mapSchema = new MessageMapSchema<>(stringSchema, stringSchema);
            this.pool = pool;
        }

        @Override
        public Map<String, String> create(byte[] content, int length) {
            Map<String, String> data = new HashMap<>();
            ProtostuffIOUtil.mergeFrom(content, 0, length, data, mapSchema);
            return data;
        }

        @Override
        public Map<String, String> create(ByteBuffer byteBuffer, int contentLength) {
            byteBuffer.flip();
            int limit = byteBuffer.limit();
            try {
                int position = byteBuffer.position();
                byteBuffer.limit(position + contentLength);
                Map<String, String> data = new HashMap<>();
                try {
                    mapSchema.mergeFrom(new ByteBufferInput(byteBuffer, true), data);
                } catch (IOException e) {
                    throw new MessageParseException(ErrorMessage.MESSAGE_PARSE_ERROR);
                }
                return data;
            } finally {
                byteBuffer.limit(limit);
            }
        }

        @Override
        public byte[] getBytes(Map<String, String> data) {
            LinkedBuffer buffer = pool.allocate();
            try {
                return ProtostuffIOUtil.toByteArray(data, mapSchema, buffer);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            } finally {
                pool.recycle(buffer);
            }
        }

        @Override
        public void getBytes(Map<String, String> obj, ByteBuf byteBuffer) {
            throw new UnsupportedOperationException();
        }
    }

}
