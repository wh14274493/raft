package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.Entry;
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
public class EntryFactory implements Factory<Entry> {


    private final Schema<Entry> schema = RuntimeSchema.getSchema(Entry.class);
    private final Pool<LinkedBuffer> pool;

    public EntryFactory(Pool<LinkedBuffer> pool) {
        this.pool = pool;
    }

    @Override
    public Entry create(byte[] content, int contentLength) {
        Entry entry = new Entry();
        ProtostuffIOUtil.mergeFrom(content, 0, contentLength, entry, schema);
        return entry;
    }

    @Override
    public Entry create(ByteBuffer byteBuffer, int contentLength) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBytes(Entry obj) {
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
    public void getBytes(Entry obj, ByteBuf byteBuffer) {
        throw new UnsupportedOperationException();
    }
}
