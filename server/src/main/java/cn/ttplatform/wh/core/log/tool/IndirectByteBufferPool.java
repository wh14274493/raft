package cn.ttplatform.wh.core.log.tool;

import cn.ttplatform.wh.support.AbstractPool;
import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/4/21 13:12
 */
public class IndirectByteBufferPool extends AbstractPool<ByteBuffer> {

    public IndirectByteBufferPool(int poolSize, int bufferSizeLimit) {
        super(poolSize, bufferSizeLimit);
    }

    @Override
    public ByteBuffer doAllocate(int size) {
        return ByteBuffer.allocate(size);
    }

    @Override
    public void recycle(ByteBuffer buffer) {
        buffer.clear();
        if (pool.size() < poolSize && buffer.capacity() <= bufferSizeLimit) {
            pool.put(buffer.capacity(), buffer);
        }
    }
}
