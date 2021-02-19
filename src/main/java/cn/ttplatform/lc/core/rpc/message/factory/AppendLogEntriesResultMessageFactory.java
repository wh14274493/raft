package cn.ttplatform.lc.core.rpc.message.factory;

import cn.ttplatform.lc.core.rpc.message.domain.AppendLogEntriesResultMessage;
import cn.ttplatform.lc.core.rpc.message.domain.Message;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:09
 */
public class AppendLogEntriesResultMessageFactory implements MessageFactory {

    private final Schema<AppendLogEntriesResultMessage> schema = RuntimeSchema
        .getSchema(AppendLogEntriesResultMessage.class);

    @Override
    public Message create(byte[] content) {
        AppendLogEntriesResultMessage message = new AppendLogEntriesResultMessage();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(LinkedBuffer buffer, Message message) {
        return ProtostuffIOUtil.toByteArray((AppendLogEntriesResultMessage) message, schema, buffer);
    }
}
