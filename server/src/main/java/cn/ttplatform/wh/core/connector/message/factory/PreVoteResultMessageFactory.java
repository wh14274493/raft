package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.support.AbstractMessageFactory;
import cn.ttplatform.wh.support.BufferPool;
import cn.ttplatform.wh.common.Message;
import cn.ttplatform.wh.core.connector.message.PreVoteResultMessage;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:18
 */
public class PreVoteResultMessageFactory extends AbstractMessageFactory {

    private final Schema<PreVoteResultMessage> schema = RuntimeSchema.getSchema(PreVoteResultMessage.class);

    public PreVoteResultMessageFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public Message create(byte[] content) {
        PreVoteResultMessage message = new PreVoteResultMessage();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Message message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((PreVoteResultMessage) message, schema, buffer);
    }
}
