package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.support.AbstractMessageFactory;
import cn.ttplatform.wh.support.BufferPool;
import cn.ttplatform.wh.support.Message;
import cn.ttplatform.wh.core.connector.message.RequestVoteResultMessage;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:21
 */
public class RequestVoteResultMessageFactory extends AbstractMessageFactory {

    private final Schema<RequestVoteResultMessage> schema = RuntimeSchema.getSchema(RequestVoteResultMessage.class);

    public RequestVoteResultMessageFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public Message create(byte[] content) {
        RequestVoteResultMessage message = new RequestVoteResultMessage();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Message message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((RequestVoteResultMessage) message, schema, buffer);
    }
}
