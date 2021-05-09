package cn.ttplatform.wh.core.log.tool;

import cn.ttplatform.wh.support.AbstractPool;
import cn.ttplatform.wh.support.PooledByteBuffer;
import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/4/19 14:45
 */
public class DirectByteBufferPool extends AbstractPool<PooledByteBuffer> {

    public DirectByteBufferPool(int poolSize, int bufferSizeLimit) {
        super(poolSize, bufferSizeLimit);
    }

    @Override
    public PooledByteBuffer doAllocate(int size) {
        return new PooledByteBuffer(ByteBuffer.allocateDirect(size),this);
    }

    @Override
    public void recycle(PooledByteBuffer buffer) {
        if (buffer != null) {
            buffer.clear();
            if (pool.size() < poolSize && buffer.capacity() <= bufferSizeLimit) {
                pool.put(buffer.capacity(), buffer);
            }
        }
    }

}
