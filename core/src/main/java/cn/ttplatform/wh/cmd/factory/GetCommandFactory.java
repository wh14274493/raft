package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.core.support.AbstractMessageFactory;
import cn.ttplatform.wh.core.support.BufferPool;
import cn.ttplatform.wh.cmd.GetCommand;
import cn.ttplatform.wh.core.connector.message.Message;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/3/15 15:50
 */
public class GetCommandFactory extends AbstractMessageFactory {

    private final Schema<GetCommand> schema = RuntimeSchema.getSchema(GetCommand.class);

    public GetCommandFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public Message create(byte[] content) {
        GetCommand message = new GetCommand();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Message message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((GetCommand) message, schema, buffer);
    }
}
