package cn.ttplatform.wh.domain.cmd.factory;

import cn.ttplatform.wh.core.common.AbstractMessageFactory;
import cn.ttplatform.wh.core.common.BufferPool;
import cn.ttplatform.wh.domain.cmd.GetCommand;
import cn.ttplatform.wh.domain.message.Message;
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
