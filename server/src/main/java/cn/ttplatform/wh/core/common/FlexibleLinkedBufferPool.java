package cn.ttplatform.wh.core.common;

import cn.ttplatform.wh.core.BufferPool;
import io.protostuff.LinkedBuffer;
import java.lang.ref.SoftReference;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Wang Hao
 * @date 2021/2/19 19:37
 */
public class FlexibleLinkedBufferPool implements BufferPool<LinkedBuffer> {

    private final Queue<SoftReference<LinkedBuffer>> pool = new LinkedBlockingQueue<>();

    @Override
    public LinkedBuffer allocate() {
        LinkedBuffer buffer;
        if (pool.isEmpty()) {
            buffer = LinkedBuffer.allocate();
        } else {
            SoftReference<LinkedBuffer> softReference = pool.poll();
            while (softReference != null && softReference.get() == null) {
                softReference = pool.poll();
            }
            buffer = softReference == null ? LinkedBuffer.allocate() : softReference.get();
        }
        return buffer;
    }

    @Override
    public void recycle(LinkedBuffer buffer) {
        buffer.clear();
        SoftReference<LinkedBuffer> softReference = new SoftReference<>(buffer);
        pool.offer(softReference);
    }
}
