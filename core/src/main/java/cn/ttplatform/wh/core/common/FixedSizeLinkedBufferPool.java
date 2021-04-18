package cn.ttplatform.wh.core.common;

import io.protostuff.LinkedBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/17 15:33
 */
@Slf4j
public class FixedSizeLinkedBufferPool implements BufferPool<LinkedBuffer> {

    private final BlockingQueue<LinkedBuffer> bufferQueue;

    public FixedSizeLinkedBufferPool(int size) {
        bufferQueue = new ArrayBlockingQueue<>(size);
    }

    @Override
    public LinkedBuffer allocate() {
        LinkedBuffer buffer = null;
        try {
            buffer = bufferQueue.poll(500L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.warn("poll LinkedBuffer timeout.");
        }
        return buffer == null ? LinkedBuffer.allocate() : buffer;
    }

    @Override
    public void recycle(LinkedBuffer buffer) {
        buffer.clear();
        try {
            bufferQueue.offer(buffer,500L,TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.warn("offer LinkedBuffer timeout.");
        }
    }

}
