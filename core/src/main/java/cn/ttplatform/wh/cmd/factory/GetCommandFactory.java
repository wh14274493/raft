package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.GetCommand;
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
 * @date 2021/3/15 15:50
 */
public class GetCommandFactory extends AbstractDistributableFactory {

    private final Schema<GetCommand> schema = RuntimeSchema.getSchema(GetCommand.class);

    public GetCommandFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public int getFactoryType() {
        return DistributableType.GET_COMMAND;
    }

    @Override
    public Distributable create(byte[] content) {
        GetCommand command = new GetCommand();
        ProtostuffIOUtil.mergeFrom(content, command, schema);
        return command;
    }

    @Override
    public byte[] getBytes(Distributable distributable, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((GetCommand) distributable, schema, buffer);
    }
}
