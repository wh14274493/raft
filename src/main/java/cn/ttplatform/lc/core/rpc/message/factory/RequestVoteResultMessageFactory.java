package cn.ttplatform.lc.core.rpc.message.factory;

import cn.ttplatform.lc.core.rpc.message.domain.Message;
import cn.ttplatform.lc.core.rpc.message.domain.RequestVoteResultMessage;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:21
 */
public class RequestVoteResultMessageFactory implements MessageFactory {

    private final Schema<RequestVoteResultMessage> schema = RuntimeSchema.getSchema(RequestVoteResultMessage.class);

    @Override
    public Message create(byte[] content) {
        RequestVoteResultMessage message = new RequestVoteResultMessage();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(LinkedBuffer buffer, Message message) {
        return ProtostuffIOUtil.toByteArray((RequestVoteResultMessage) message, schema, buffer);
    }

    public static void main(String[] args) {

    }
}
