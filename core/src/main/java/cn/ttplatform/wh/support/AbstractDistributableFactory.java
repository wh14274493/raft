package cn.ttplatform.wh.support;

import io.protostuff.LinkedBuffer;

/**
 * @author Wang Hao
 * @date 2021/3/15 14:25
 */
public abstract class AbstractDistributableFactory implements DistributableFactory {

    BufferPool<LinkedBuffer> pool;

    protected AbstractDistributableFactory(BufferPool<LinkedBuffer> pool) {
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
}
