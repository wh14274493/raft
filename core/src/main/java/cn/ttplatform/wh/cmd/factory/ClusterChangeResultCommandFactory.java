package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.ClusterChangeResultCommand;
import cn.ttplatform.wh.support.Message;
import cn.ttplatform.wh.support.AbstractMessageFactory;
import cn.ttplatform.wh.support.BufferPool;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/4/24 11:30
 */
public class ClusterChangeResultCommandFactory extends AbstractMessageFactory {

    private final Schema<ClusterChangeResultCommand> schema = RuntimeSchema.getSchema(ClusterChangeResultCommand.class);

    public ClusterChangeResultCommandFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public Message create(byte[] content) {
        ClusterChangeResultCommand message = new ClusterChangeResultCommand();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Message message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((ClusterChangeResultCommand) message, schema, buffer);
    }
}
