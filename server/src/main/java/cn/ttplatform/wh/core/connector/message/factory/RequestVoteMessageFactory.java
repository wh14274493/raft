package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.support.AbstractMessageFactory;
import cn.ttplatform.wh.support.BufferPool;
import cn.ttplatform.wh.cmd.Message;
import cn.ttplatform.wh.core.connector.message.RequestVoteMessage;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:19
 */
public class RequestVoteMessageFactory extends AbstractMessageFactory {

    private final Schema<RequestVoteMessage> schema = RuntimeSchema.getSchema(RequestVoteMessage.class);

    public RequestVoteMessageFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public Message create(byte[] content) {
        RequestVoteMessage message = new RequestVoteMessage();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Message message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((RequestVoteMessage) message, schema, buffer);
    }
}
