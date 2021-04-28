package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.GetClusterInfoResultCommand;
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
public class GetClusterInfoResultCommandFactory extends AbstractDistributableFactory {

    private final Schema<GetClusterInfoResultCommand> schema = RuntimeSchema.getSchema(GetClusterInfoResultCommand.class);

    public GetClusterInfoResultCommandFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public int getFactoryType() {
        return DistributableType.GET_CLUSTER_INFO_RESULT_COMMAND;
    }

    @Override
    public Distributable create(byte[] content) {
        GetClusterInfoResultCommand command = new GetClusterInfoResultCommand();
        ProtostuffIOUtil.mergeFrom(content, command, schema);
        return command;
    }

    @Override
    public byte[] getBytes(Distributable distributable, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((GetClusterInfoResultCommand) distributable, schema, buffer);
    }
}
