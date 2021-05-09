package cn.ttplatform.wh.support;

import io.protostuff.LinkedBuffer;
import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/3/15 14:25
 */
public abstract class AbstractDistributableFactory implements DistributableFactory {

    Pool<LinkedBuffer> pool;

    protected AbstractDistributableFactory(Pool<LinkedBuffer> pool) {
        this.pool = pool;
    }

    @Override
    public byte[] getBytes(Distributable distributable) {
        LinkedBuffer buffer = pool.allocate();
        try {
            return getBytes(distributable, buffer);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            pool.recycle(buffer);
        }
    }

    public abstract byte[] getBytes(Distributable distributable, LinkedBuffer buffer);


    @Override
    public Distributable create(ByteBuffer byteBuffer, int contentLength) {
        int limit = byteBuffer.limit();
        try {
            int position = byteBuffer.position();
            byteBuffer.limit(position + contentLength);
            return create(byteBuffer);
        } finally {
            byteBuffer.limit(limit);
        }
    }

    public abstract Distributable create(ByteBuffer byteBuffer);
}
