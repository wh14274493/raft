package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.Message;
import cn.ttplatform.wh.cmd.SetResponseCommand;
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
public class SetResponseCommandMessageFactory extends AbstractMessageFactory {

    private final Schema<SetResponseCommand> schema = RuntimeSchema.getSchema(SetResponseCommand.class);

    public SetResponseCommandMessageFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public Message create(byte[] content) {
        SetResponseCommand message = new SetResponseCommand();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Message message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((SetResponseCommand) message, schema, buffer);
    }
}
