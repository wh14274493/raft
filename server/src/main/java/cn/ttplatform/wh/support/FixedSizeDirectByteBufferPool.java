package cn.ttplatform.wh.support;

import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/6/10 21:04
 */
public class FixedSizeDirectByteBufferPool extends AbstractFixedSizeByteBufferPool {

    public FixedSizeDirectByteBufferPool(int poolSize, int chunkSize) {
        super(poolSize, chunkSize);
    }

    @Override
    public ByteBuffer doAllocate() {
        return ByteBuffer.allocateDirect(chunkSize);
    }
}
