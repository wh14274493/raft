package cn.ttplatform.wh.core.support;

import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/4/21 13:12
 */
public class IndirectByteBufferPool extends AbstractByteBufferPool {

    public IndirectByteBufferPool(int poolSize, int bufferSizeLimit) {
        super(poolSize, bufferSizeLimit);
    }

    @Override
    public ByteBuffer doAllocate(int size) {
        return ByteBuffer.allocate(size);
    }
}
