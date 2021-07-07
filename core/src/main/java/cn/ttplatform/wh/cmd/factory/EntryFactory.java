package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.KeyValuePair;
import cn.ttplatform.wh.support.Factory;
import cn.ttplatform.wh.support.Pool;
import io.netty.buffer.ByteBuf;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/5/19 21:54
 */
public class EntryFactory implements Factory<KeyValuePair> {


    private final Schema<KeyValuePair> schema = RuntimeSchema.getSchema(KeyValuePair.class);
    private final Pool<LinkedBuffer> pool;

    public EntryFactory(Pool<LinkedBuffer> pool) {
        this.pool = pool;
    }

    @Override
    public KeyValuePair create(byte[] content, int contentLength) {
        KeyValuePair keyValuePair = new KeyValuePair();
        ProtostuffIOUtil.mergeFrom(content, 0, contentLength, keyValuePair, schema);
        return keyValuePair;
    }

    @Override
    public KeyValuePair create(ByteBuffer byteBuffer, int contentLength) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBytes(KeyValuePair obj) {
        LinkedBuffer buffer = pool.allocate();
        try {
            return ProtostuffIOUtil.toByteArray(obj, schema, buffer);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            pool.recycle(buffer);
        }
    }

    @Override
    public void getBytes(KeyValuePair obj, ByteBuf byteBuffer) {
        throw new UnsupportedOperationException();
    }
}
