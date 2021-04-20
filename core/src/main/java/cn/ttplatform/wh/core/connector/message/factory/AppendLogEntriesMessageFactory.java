package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.core.support.BufferPool;
import cn.ttplatform.wh.core.support.AbstractMessageFactory;
import cn.ttplatform.wh.core.connector.message.AppendLogEntriesMessage;
import cn.ttplatform.wh.core.connector.message.Message;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 11:27
 */
public class AppendLogEntriesMessageFactory extends AbstractMessageFactory {

    private final Schema<AppendLogEntriesMessage> schema = RuntimeSchema.getSchema(AppendLogEntriesMessage.class);

    public AppendLogEntriesMessageFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public Message create(byte[] content) {
        AppendLogEntriesMessage message = new AppendLogEntriesMessage();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Message message,LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((AppendLogEntriesMessage) message, schema, buffer);
    }

}
