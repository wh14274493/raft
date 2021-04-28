package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.GetResultCommand;
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
public class GetResultCommandFactory extends AbstractDistributableFactory {

    private final Schema<GetResultCommand> schema = RuntimeSchema.getSchema(GetResultCommand.class);

    public GetResultCommandFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public int getFactoryType() {
        return DistributableType.GET_COMMAND_RESULT;
    }

    @Override
    public Distributable create(byte[] content) {
        GetResultCommand command = new GetResultCommand();
        ProtostuffIOUtil.mergeFrom(content, command, schema);
        return command;
    }

    @Override
    public byte[] getBytes(Distributable distributable, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((GetResultCommand) distributable, schema, buffer);
    }
}
