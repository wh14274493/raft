package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.SetResultCommand;
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
 * @date 2021/4/16 22:49
 */
public class SetResultCommandFactory extends AbstractDistributableFactory {

    private final Schema<SetResultCommand> schema = RuntimeSchema.getSchema(SetResultCommand.class);

    public SetResultCommandFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public int getFactoryType() {
        return DistributableType.SET_COMMAND_RESULT;
    }

    @Override
    public Distributable create(byte[] content) {
        SetResultCommand command = new SetResultCommand();
        ProtostuffIOUtil.mergeFrom(content, command, schema);
        return command;
    }

    @Override
    public byte[] getBytes(Distributable distributable, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((SetResultCommand) distributable, schema, buffer);
    }
}
