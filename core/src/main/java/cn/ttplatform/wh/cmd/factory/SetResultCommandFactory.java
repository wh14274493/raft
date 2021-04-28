package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.support.Message;
import cn.ttplatform.wh.cmd.SetResultCommand;
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
public class SetResultCommandFactory extends AbstractMessageFactory {

    private final Schema<SetResultCommand> schema = RuntimeSchema.getSchema(SetResultCommand.class);

    public SetResultCommandFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public Message create(byte[] content) {
        SetResultCommand message = new SetResultCommand();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Message message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((SetResultCommand) message, schema, buffer);
    }
}
