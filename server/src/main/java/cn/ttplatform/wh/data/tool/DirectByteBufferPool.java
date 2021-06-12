package cn.ttplatform.wh.data.tool;

import cn.ttplatform.wh.support.AbstractByteBufferPool;
import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/4/19 14:45
 */
public class DirectByteBufferPool extends AbstractByteBufferPool {

    public DirectByteBufferPool(int poolSize, int bufferSizeLimit) {
        super(poolSize, bufferSizeLimit);
    }

    @Override
    public ByteBuffer doAllocate(int size) {
        return ByteBuffer.allocateDirect(size);
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
