package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.core.support.AbstractMessageFactory;
import cn.ttplatform.wh.core.support.BufferPool;
import cn.ttplatform.wh.cmd.SetCommand;
import cn.ttplatform.wh.core.connector.message.Message;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/3/15 15:56
 */
public class SetCommandFactory extends AbstractMessageFactory {

    private final Schema<SetCommand> schema = RuntimeSchema.getSchema(SetCommand.class);

    public SetCommandFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public Message create(byte[] content) {
        SetCommand message = new SetCommand();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        message.setCmd(content);
        return message;
    }

    @Override
    public byte[] getBytes(Message message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((SetCommand) message, schema, buffer);
    }
}
