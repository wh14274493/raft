package cn.ttplatform.wh.support;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Wang Hao
 * @date 2021/6/10 20:49
 */
public abstract class AbstractFixedSizeByteBufferPool implements Pool<ByteBuffer> {

    protected final int poolSize;
    protected final int chunkSize;
    protected final Queue<ByteBuffer> queue;

    protected AbstractFixedSizeByteBufferPool(int poolSize, int chunkSize) {
        this.poolSize = poolSize;
        this.chunkSize = chunkSize;
        this.queue = new LinkedList<>();
    }

    @Override
    public ByteBuffer allocate() {
        if (!queue.isEmpty()) {
            return queue.poll();
        }
        return doAllocate();
    }

    /**
     * Reallocate a new chunk of memory
     *
     * @return a new buffer
     */
    public abstract ByteBuffer doAllocate();

    @Override
    public void recycle(ByteBuffer buffer) {
        if (queue.size() < poolSize) {
            queue.offer(buffer);
        }
    }
}
