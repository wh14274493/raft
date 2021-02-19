package cn.ttplatform.lc.core.rpc.message.factory;

import cn.ttplatform.lc.core.rpc.message.domain.Message;
import cn.ttplatform.lc.core.rpc.message.domain.PreVoteMessage;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:16
 */
public class PreVoteMessageFactory implements MessageFactory {

    private final Schema<PreVoteMessage> schema = RuntimeSchema.getSchema(PreVoteMessage.class);

    @Override
    public Message create(byte[] content) {
        PreVoteMessage message = new PreVoteMessage();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(LinkedBuffer buffer, Message message) {
        return ProtostuffIOUtil.toByteArray((PreVoteMessage) message, schema, buffer);
    }
}
