package cn.ttplatform.wh.core.support;

import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * @author Wang Hao
 * @date 2021/4/21 13:07
 */
public abstract class AbstractByteBufferPool implements BufferPool<ByteBuffer> {

    private final int bufferSizeLimit;
    private final int poolSize;
    private final TreeMap<Integer, ByteBuffer> pool;

    protected AbstractByteBufferPool(int poolSize, int bufferSizeLimit) {
        this.bufferSizeLimit = bufferSizeLimit;
        this.poolSize = poolSize;
        pool = new TreeMap<>();
    }

    @Override
    public ByteBuffer allocate(int size) {
        if (!pool.isEmpty()) {
            Entry<Integer, ByteBuffer> bufferEntry = pool.ceilingEntry(size);
            if (bufferEntry != null) {
                return pool.remove(bufferEntry.getKey());
            }
        }
        return doAllocate(size);
    }


    public abstract ByteBuffer doAllocate(int size);

    @Override
    public void recycle(ByteBuffer buffer) {
        buffer.clear();
        if (pool.size() < poolSize && buffer.capacity() <= bufferSizeLimit) {
            pool.put(buffer.capacity(), buffer);
        }
    }
}
