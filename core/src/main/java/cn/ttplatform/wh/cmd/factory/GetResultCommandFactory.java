package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.GetResultCommand;
import cn.ttplatform.wh.support.Message;
import cn.ttplatform.wh.support.AbstractMessageFactory;
import cn.ttplatform.wh.support.BufferPool;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/4/16 22:49
 */
public class GetResultCommandFactory extends AbstractMessageFactory {
    private final Schema<GetResultCommand> schema = RuntimeSchema.getSchema(GetResultCommand.class);

    public GetResultCommandFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public Message create(byte[] content) {
        GetResultCommand message = new GetResultCommand();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Message message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((GetResultCommand) message, schema, buffer);
    }
}
