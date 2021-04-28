package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.SetCommand;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.support.AbstractDistributableFactory;
import cn.ttplatform.wh.support.BufferPool;
import cn.ttplatform.wh.support.Distributable;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/3/15 15:56
 */
public class SetCommandFactory extends AbstractDistributableFactory {

    private final Schema<SetCommand> schema = RuntimeSchema.getSchema(SetCommand.class);

    public SetCommandFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public int getFactoryType() {
        return DistributableType.SET_COMMAND;
    }

    @Override
    public Distributable create(byte[] content) {
        SetCommand command = new SetCommand();
        ProtostuffIOUtil.mergeFrom(content, command, schema);
        command.setCmd(content);
        return command;
    }

    @Override
    public byte[] getBytes(Distributable message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((SetCommand) message, schema, buffer);
    }
}
