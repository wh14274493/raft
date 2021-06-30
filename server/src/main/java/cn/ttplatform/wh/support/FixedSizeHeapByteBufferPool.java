package cn.ttplatform.wh.support;

import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/6/10 21:05
 */
public class FixedSizeHeapByteBufferPool extends AbstractFixedSizeByteBufferPool {

    protected FixedSizeHeapByteBufferPool(int poolSize, int chunkSize) {
        super(poolSize, chunkSize);
    }

    @Override
    public ByteBuffer doAllocate() {
        return ByteBuffer.allocate(chunkSize);
    }
}
