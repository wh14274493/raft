package cn.ttplatform.wh.data.tool;

import cn.ttplatform.wh.support.AbstractByteBufferPool;
import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/4/21 13:12
 */
public class HeapByteBufferPool extends AbstractByteBufferPool {

    public HeapByteBufferPool(int poolSize, int bufferSizeLimit) {
        super(poolSize, bufferSizeLimit);
    }

    @Override
    public ByteBuffer doAllocate(int size) {
        return ByteBuffer.allocate(size);
    }

    @Override
    public void recycle(ByteBuffer buffer) {
        if (buffer != null) {
            buffer.clear();
            if (pool.size() < poolSize && buffer.capacity() <= bufferSizeLimit) {
                pool.put(buffer.capacity(), buffer);
            }
        }
    }
}
