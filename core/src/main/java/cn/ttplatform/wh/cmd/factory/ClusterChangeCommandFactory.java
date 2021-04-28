package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.ClusterChangeCommand;
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
 * @date 2021/4/24 11:30
 */
public class ClusterChangeCommandFactory extends AbstractDistributableFactory {

    private final Schema<ClusterChangeCommand> schema = RuntimeSchema.getSchema(ClusterChangeCommand.class);

    public ClusterChangeCommandFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public int getFactoryType() {
        return DistributableType.CLUSTER_CHANGE_COMMAND;
    }

    @Override
    public Distributable create(byte[] content) {
        ClusterChangeCommand command = new ClusterChangeCommand();
        ProtostuffIOUtil.mergeFrom(content, command, schema);
        return command;
    }

    @Override
    public byte[] getBytes(Distributable distributable, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((ClusterChangeCommand) distributable, schema, buffer);
    }
}
