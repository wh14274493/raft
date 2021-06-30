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
    protected final int defaultChunkSize;
    protected final TreeMap<Integer, ByteBuffer> pool;

    protected AbstractByteBufferPool(int poolSize, int defaultChunkSize, int bufferSizeLimit) {
        this.bufferSizeLimit = bufferSizeLimit;
        this.poolSize = poolSize;
        this.defaultChunkSize = defaultChunkSize;
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

    @Override
    public ByteBuffer allocate() {
        ByteBuffer byteBuffer = pool.remove(defaultChunkSize);
        if (byteBuffer != null) {
            return byteBuffer;
        }
        return doAllocate(defaultChunkSize);
    }

    /**
     * Reallocate a new chunk of memory
     *
     * @param size chunk size
     * @return a new buffer
     */
    public abstract ByteBuffer doAllocate(int size);

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
