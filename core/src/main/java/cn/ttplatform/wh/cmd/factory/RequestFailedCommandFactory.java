package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.RequestFailedCommand;
import cn.ttplatform.wh.core.connector.message.Message;
import cn.ttplatform.wh.core.support.AbstractMessageFactory;
import cn.ttplatform.wh.core.support.BufferPool;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/4/20 12:56
 */
public class RequestFailedCommandFactory extends AbstractMessageFactory {

    private final Schema<RequestFailedCommand> schema = RuntimeSchema.getSchema(RequestFailedCommand.class);

    public RequestFailedCommandFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public byte[] getBytes(Message message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((RequestFailedCommand) message, schema, buffer);
    }

    @Override
    public Message create(byte[] content) {
        RequestFailedCommand message = new RequestFailedCommand();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }
}
