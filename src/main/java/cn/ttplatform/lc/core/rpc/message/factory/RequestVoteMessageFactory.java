package cn.ttplatform.lc.core.rpc.message.factory;

import cn.ttplatform.lc.core.rpc.message.domain.Message;
import cn.ttplatform.lc.core.rpc.message.domain.RequestVoteMessage;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:19
 */
public class RequestVoteMessageFactory implements MessageFactory {

    private final Schema<RequestVoteMessage> schema = RuntimeSchema.getSchema(RequestVoteMessage.class);

    @Override
    public Message create(byte[] content) {
        RequestVoteMessage message = new RequestVoteMessage();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(LinkedBuffer buffer, Message message) {
        return ProtostuffIOUtil.toByteArray((RequestVoteMessage) message, schema, buffer);
    }
}
