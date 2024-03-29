package cn.ttplatform.wh;

import cn.ttplatform.wh.constant.ErrorMessage;
import cn.ttplatform.wh.exception.MessageParseException;
import cn.ttplatform.wh.support.Factory;
import cn.ttplatform.wh.support.Pool;
import io.netty.buffer.ByteBuf;
import io.protostuff.*;
import io.protostuff.runtime.RuntimeSchema;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Wang Hao
 * @date 2021/2/21 16:16
 */
@Slf4j
public class StateMachine {

    private Map<String, String> data = new HashMap<>(512);
    private final Map<String, String> tempBuffer = new HashMap<>(128);
    private final DataFactory dataFactory;
    private volatile boolean generating;
    private volatile int applied;

    public StateMachine(GlobalContext context) {
        this.dataFactory = new DataFactory(context.getLinkedBufferPool());
    }

    public StateMachine(Pool<LinkedBuffer> pool) {
        this.dataFactory = new DataFactory(pool);
    }

    public String get(String key) {
        if (generating && tempBuffer.containsKey(key)) {
            return tempBuffer.get(key);
        }
        return data.get(key);
    }

    public void set(String key, String value) {
        synchronized (tempBuffer) {
            if (generating) {
                tempBuffer.put(key, value);
                return;
            }
            if (value == null) {
                data.remove(key);
            } else {
                data.put(key, value);
            }
        }
    }

    public int getPairs() {
        return data.size();
    }

    public int getApplied() {
        return applied;
    }

    public void setApplied(int applied) {
        if (applied <= this.applied) {
            return;
        }
        this.applied = applied;
    }

    public boolean startGenerating() {
        boolean old = generating;
        this.generating = true;
        return !old;
    }

    public void stopGenerating() {
        this.generating = false;
        synchronized (tempBuffer) {
            tempBuffer.forEach((k, v) -> {
                if (v == null) {
                    data.remove(k);
                } else {
                    data.put(k, v);
                }
            });
            tempBuffer.clear();
        }
    }

    public byte[] generateSnapshotData() {
        return dataFactory.getBytes(data);
    }

    public void applySnapshotData(ByteBuffer snapshot, int lastIncludeIndex) {
        data = dataFactory.create(snapshot, snapshot.limit());
        applied = lastIncludeIndex;
        log.info("apply snapshot that lastIncludeIndex is {}.", lastIncludeIndex);
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
            Map<String, String> data = new HashMap<>(512);
            ProtostuffIOUtil.mergeFrom(content, 0, length, data, mapSchema);
            return data;
        }

        @Override
        public Map<String, String> create(ByteBuffer byteBuffer, int contentLength) {
            int limit = byteBuffer.limit();
            try {
                int position = byteBuffer.position();
                byteBuffer.limit(position + contentLength);
                Map<String, String> data = new HashMap<>(512);
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
