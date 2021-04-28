package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.GetClusterInfoCommand;
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
 * @date 2021/4/28 10:54
 */
public class GetClusterInfoCommandFactory extends AbstractDistributableFactory {

    private final Schema<GetClusterInfoCommand> schema = RuntimeSchema.getSchema(GetClusterInfoCommand.class);

    public GetClusterInfoCommandFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public int getFactoryType() {
        return DistributableType.GET_CLUSTER_INFO_COMMAND;
    }

    @Override
    public Distributable create(byte[] content) {
        GetClusterInfoCommand command = new GetClusterInfoCommand();
        ProtostuffIOUtil.mergeFrom(content, command, schema);
        return command;
    }

    @Override
    public byte[] getBytes(Distributable distributable, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((GetClusterInfoCommand) distributable, schema, buffer);
    }
}
