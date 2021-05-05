package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.SetCommand;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.support.AbstractDistributableFactory;
import cn.ttplatform.wh.support.Pool;
import cn.ttplatform.wh.support.Distributable;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import java.util.Arrays;

/**
 * @author Wang Hao
 * @date 2021/3/15 15:56
 */
public class SetCommandFactory extends AbstractDistributableFactory {

    private final Schema<SetCommand> schema = RuntimeSchema.getSchema(SetCommand.class);

    public SetCommandFactory(Pool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public int getFactoryType() {
        return DistributableType.SET_COMMAND;
    }

    @Override
    public Distributable create(byte[] content, int length) {
        SetCommand command = new SetCommand();
        ProtostuffIOUtil.mergeFrom(content, 0, length, command, schema);
        command.setCmd(Arrays.copyOfRange(content, 0, length));
        return command;
    }

    @Override
    public byte[] getBytes(Distributable message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((SetCommand) message, schema, buffer);
    }
}
