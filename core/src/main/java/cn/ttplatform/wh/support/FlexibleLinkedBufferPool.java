package cn.ttplatform.wh.support;

import io.protostuff.LinkedBuffer;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/19 19:37
 */
@Slf4j
public class FlexibleLinkedBufferPool implements Pool<LinkedBuffer> {

    private final BlockingQueue<SoftReference<LinkedBuffer>> pool = new LinkedBlockingQueue<>();
    private final ReferenceQueue<LinkedBuffer> referenceQueue = new ReferenceQueue<>();

    @Override
    public LinkedBuffer allocate() {
        LinkedBuffer buffer;
        if (pool.isEmpty()) {
            buffer = LinkedBuffer.allocate();
        } else {
            synchronized (referenceQueue) {
                Reference<? extends LinkedBuffer> reference;
                while ((reference = referenceQueue.poll()) != null) {
                    log.debug("remove result is {}.", pool.remove(reference));
                }
            }
            SoftReference<LinkedBuffer> softReference = null;
            try {
                softReference = pool.poll(500L, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                log.warn("failed to poll LinkedBuffer.");
                Thread.currentThread().interrupt();
            }
            buffer = softReference == null ? LinkedBuffer.allocate() : softReference.get();
        }
        return buffer;
    }

    @Override
    public void recycle(LinkedBuffer buffer) {
        buffer.clear();
        SoftReference<LinkedBuffer> softReference = new SoftReference<>(buffer, referenceQueue);
        try {
            if (pool.offer(softReference, 500L, TimeUnit.MILLISECONDS)) {
                log.debug("offer LinkedBuffer in 500ms.");
            }
        } catch (Exception e) {
            log.warn("offer LinkedBuffer timeout.");
            Thread.currentThread().interrupt();
        }
    }
}
