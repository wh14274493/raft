package cn.ttplatform.lc.core.rpc.message;

import io.protostuff.LinkedBuffer;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author Wang Hao
 * @date 2021/2/17 15:33
 */
public class LinkedBufferPool {

    private final Queue<LinkedBuffer> bufferQueue;

    public LinkedBufferPool(int size) {
        bufferQueue = new ArrayBlockingQueue<>(size);
        for (int i = 0; i < size; i++) {
            bufferQueue.offer(LinkedBuffer.allocate());
        }
    }

    public LinkedBuffer allocate() {
        return bufferQueue.poll();
    }

    public void recycle(LinkedBuffer buffer) {
        buffer.clear();
        bufferQueue.offer(buffer);
    }

}
