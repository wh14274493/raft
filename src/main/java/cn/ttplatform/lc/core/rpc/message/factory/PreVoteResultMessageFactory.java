package cn.ttplatform.lc.core.rpc.message.factory;

import cn.ttplatform.lc.core.rpc.message.domain.Message;
import cn.ttplatform.lc.core.rpc.message.domain.PreVoteResultMessage;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:18
 */
public class PreVoteResultMessageFactory implements MessageFactory {

    private final Schema<PreVoteResultMessage> schema = RuntimeSchema.getSchema(PreVoteResultMessage.class);

    @Override
    public Message create(byte[] content) {
        PreVoteResultMessage message = new PreVoteResultMessage();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(LinkedBuffer buffer, Message message) {
        return ProtostuffIOUtil.toByteArray((PreVoteResultMessage) message, schema, buffer);
    }
}
