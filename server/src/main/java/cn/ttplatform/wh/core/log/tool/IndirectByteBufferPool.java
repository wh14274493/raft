package cn.ttplatform.wh.core.log.tool;

import cn.ttplatform.wh.support.AbstractPool;
import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/4/21 13:12
 */
public class IndirectByteBufferPool extends AbstractPool<PooledByteBuffer> {

    public IndirectByteBufferPool(int poolSize, int bufferSizeLimit) {
        super(poolSize, bufferSizeLimit);
    }

    @Override
    public PooledByteBuffer doAllocate(int size) {
        return new PooledByteBuffer(ByteBuffer.allocate(size), this);
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
