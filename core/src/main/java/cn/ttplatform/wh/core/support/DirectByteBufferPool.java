package cn.ttplatform.wh.core.support;

import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * @author Wang Hao
 * @date 2021/4/19 14:45
 */
public class DirectByteBufferPool implements BufferPool<ByteBuffer> {

    private final int bufferSizeLimit;
    private final int poolSize;
    private final TreeMap<Integer, ByteBuffer> pool;

    public DirectByteBufferPool(int poolSize,int bufferSizeLimit) {
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
        return ByteBuffer.allocateDirect(size);
    }

    @Override
    public void recycle(ByteBuffer buffer) {
        buffer.clear();
        if (pool.size() < poolSize && buffer.capacity() <= bufferSizeLimit) {
            pool.put(buffer.capacity(), buffer);
        }
    }
}
