package cn.ttplatform.wh.support;

import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/4/19 14:45
 */
public class DirectByteBufferPool extends AbstractByteBufferPool {

    public DirectByteBufferPool(int poolSize, int defaultChunkSize, int bufferSizeLimit) {
        super(poolSize, defaultChunkSize, bufferSizeLimit);
    }

    @Override
    public ByteBuffer doAllocate(int size) {
        return ByteBuffer.allocateDirect(size);
    }

}
