package cn.ttplatform.lc.core.rpc.message.factory;

import cn.ttplatform.lc.core.rpc.message.domain.AppendLogEntriesMessage;
import cn.ttplatform.lc.core.rpc.message.domain.Message;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 11:27
 */
public class AppendLogEntriesMessageFactory implements MessageFactory {

    private final Schema<AppendLogEntriesMessage> schema = RuntimeSchema.getSchema(AppendLogEntriesMessage.class);

    @Override
    public Message create(byte[] content) {
        AppendLogEntriesMessage message = new AppendLogEntriesMessage();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(LinkedBuffer buffer, Message message) {
        return ProtostuffIOUtil.toByteArray((AppendLogEntriesMessage) message, schema, buffer);
    }

}
