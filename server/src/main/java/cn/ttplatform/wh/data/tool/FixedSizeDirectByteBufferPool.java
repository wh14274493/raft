package cn.ttplatform.wh.data.tool;

import cn.ttplatform.wh.support.AbstractFixedSizeByteBufferPool;
import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/6/10 21:04
 */
public class FixedSizeDirectByteBufferPool extends AbstractFixedSizeByteBufferPool {

    protected FixedSizeDirectByteBufferPool(int poolSize, int chunkSize) {
        super(poolSize, chunkSize);
    }

    @Override
    public ByteBuffer doAllocate() {
        return ByteBuffer.allocateDirect(chunkSize);
    }
}
