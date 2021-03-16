package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.core.BufferPool;
import cn.ttplatform.wh.core.common.AbstractMessageFactory;
import cn.ttplatform.wh.domain.message.AppendLogEntriesResultMessage;
import cn.ttplatform.wh.domain.message.Message;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:09
 */
public class AppendLogEntriesResultMessageFactory extends AbstractMessageFactory {

    private final Schema<AppendLogEntriesResultMessage> schema = RuntimeSchema
        .getSchema(AppendLogEntriesResultMessage.class);

    public AppendLogEntriesResultMessageFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public Message create(byte[] content) {
        AppendLogEntriesResultMessage message = new AppendLogEntriesResultMessage();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Message message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((AppendLogEntriesResultMessage) message, schema, buffer);
    }
}
