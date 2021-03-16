package cn.ttplatform.wh.core.common;

import cn.ttplatform.wh.core.BufferPool;
import io.protostuff.LinkedBuffer;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author Wang Hao
 * @date 2021/2/17 15:33
 */
public class FixedSizeLinkedBufferPool implements BufferPool<LinkedBuffer> {

    private final Queue<LinkedBuffer> bufferQueue;

    public FixedSizeLinkedBufferPool(int size) {
        bufferQueue = new ArrayBlockingQueue<>(size);
    }

    @Override
    public LinkedBuffer allocate() {
        LinkedBuffer buffer = bufferQueue.poll();
        return buffer == null ? LinkedBuffer.allocate() : buffer;
    }

    @Override
    public void recycle(LinkedBuffer buffer) {
        buffer.clear();
        bufferQueue.offer(buffer);
    }

}
