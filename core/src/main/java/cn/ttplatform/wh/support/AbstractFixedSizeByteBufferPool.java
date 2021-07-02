package cn.ttplatform.wh.support;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

/**
 * @author Wang Hao
 * @date 2021/6/10 20:49
 */
public abstract class AbstractFixedSizeByteBufferPool implements Pool<ByteBuffer> {

    private final int poolSize;
    protected final int chunkSize;
    private final Queue<ByteBuffer> queue;
    private final Set<ByteBuffer> set;

    protected AbstractFixedSizeByteBufferPool(int poolSize, int chunkSize) {
        this.poolSize = poolSize;
        this.chunkSize = chunkSize;
        this.queue = new LinkedList<>();
        this.set = new HashSet<>((int) (poolSize / 0.75f + 1));
    }

    @Override
    public ByteBuffer allocate() {
        if (!queue.isEmpty()) {
            ByteBuffer poll = queue.poll();
            set.remove(poll);
            return poll;
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
        if (buffer != null) {
            buffer.clear();
            if (queue.size() < poolSize && !set.contains(buffer)) {
                queue.offer(buffer);
            }
        }
    }
}
