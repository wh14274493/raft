package cn.ttplatform.wh.core.support;

import cn.ttplatform.wh.core.connector.message.Message;
import io.protostuff.LinkedBuffer;

/**
 * @author Wang Hao
 * @date 2021/3/15 14:25
 */
public abstract class AbstractMessageFactory implements MessageFactory {

    BufferPool<LinkedBuffer> pool;

    protected AbstractMessageFactory(BufferPool<LinkedBuffer> pool) {
        this.pool = pool;
    }

    @Override
    public byte[] getBytes(Message message) {
        LinkedBuffer buffer = pool.allocate();
        try {
            return getBytes(message, buffer);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            pool.recycle(buffer);
        }
    }

    public abstract byte[] getBytes(Message message, LinkedBuffer buffer);
}
