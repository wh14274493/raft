package cn.ttplatform.wh.support;

import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * @author Wang Hao
 * @date 2021/4/21 13:07
 */
public abstract class AbstractByteBufferPool implements Pool<ByteBuffer> {

    protected final int bufferSizeLimit;
    protected final int poolSize;
    protected final TreeMap<Integer, ByteBuffer> pool;

    protected AbstractByteBufferPool(int poolSize, int bufferSizeLimit) {
        this.bufferSizeLimit = bufferSizeLimit;
        this.poolSize = poolSize;
        this.pool = new TreeMap<>();
    }

    @Override
    public ByteBuffer allocate(int size) {
        if (!pool.isEmpty()) {
            Entry<Integer, ByteBuffer> bufferEntry = pool.ceilingEntry(size);
            if (bufferEntry != null) {
                ByteBuffer byteBuffer = pool.remove(bufferEntry.getKey());
                byteBuffer.limit(size);
                return byteBuffer;
            }
        }
        return doAllocate(size);
    }

    /**
     * Reallocate a new chunk of memory
     *
     * @param size chunk size
     * @return a new buffer
     */
    public abstract ByteBuffer doAllocate(int size);
}
