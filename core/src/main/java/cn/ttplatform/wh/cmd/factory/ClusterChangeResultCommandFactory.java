package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.ClusterChangeResultCommand;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.support.AbstractDistributableFactory;
import cn.ttplatform.wh.support.Pool;
import cn.ttplatform.wh.support.Distributable;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/4/24 11:30
 */
public class ClusterChangeResultCommandFactory extends AbstractDistributableFactory {

    private final Schema<ClusterChangeResultCommand> schema = RuntimeSchema.getSchema(ClusterChangeResultCommand.class);

    public ClusterChangeResultCommandFactory(Pool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public int getFactoryType() {
        return DistributableType.CLUSTER_CHANGE_RESULT_COMMAND;
    }

    @Override
    public Distributable create(byte[] content, int length) {
        ClusterChangeResultCommand command = new ClusterChangeResultCommand();
        ProtostuffIOUtil.mergeFrom(content, 0, length, command, schema);
        return command;
    }

    @Override
    public byte[] getBytes(Distributable distributable, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((ClusterChangeResultCommand) distributable, schema, buffer);
    }
}
