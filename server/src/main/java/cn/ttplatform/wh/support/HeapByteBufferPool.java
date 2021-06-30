package cn.ttplatform.wh.support;

import cn.ttplatform.wh.support.AbstractByteBufferPool;

import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/4/21 13:12
 */
public class HeapByteBufferPool extends AbstractByteBufferPool {

    public HeapByteBufferPool(int poolSize, int defaultChunkSize, int bufferSizeLimit) {
        super(poolSize, defaultChunkSize, bufferSizeLimit);
    }

    @Override
    public ByteBuffer doAllocate(int size) {
        return ByteBuffer.allocate(size);
    }

}
