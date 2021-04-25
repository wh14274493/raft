package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.ClusterChangeCommand;
import cn.ttplatform.wh.common.Message;
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
public class ClusterChangeCommandFactory extends AbstractMessageFactory {

    private final Schema<ClusterChangeCommand> schema = RuntimeSchema.getSchema(ClusterChangeCommand.class);

    protected ClusterChangeCommandFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public Message create(byte[] content) {
        ClusterChangeCommand message = new ClusterChangeCommand();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        message.setCmd(content);
        return message;
    }

    @Override
    public byte[] getBytes(Message message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((ClusterChangeCommand) message, schema, buffer);
    }
}
