package cn.ttplatform.wh.core.log.tool;

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
}
