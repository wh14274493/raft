package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.core.BufferPool;
import cn.ttplatform.wh.core.common.AbstractMessageFactory;
import cn.ttplatform.wh.domain.message.Message;
import cn.ttplatform.wh.domain.message.PreVoteMessage;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:16
 */
public class PreVoteMessageFactory extends AbstractMessageFactory {

    private final Schema<PreVoteMessage> schema = RuntimeSchema.getSchema(PreVoteMessage.class);

    public PreVoteMessageFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public Message create(byte[] content) {
        PreVoteMessage message = new PreVoteMessage();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Message message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((PreVoteMessage) message, schema, buffer);
    }
}
