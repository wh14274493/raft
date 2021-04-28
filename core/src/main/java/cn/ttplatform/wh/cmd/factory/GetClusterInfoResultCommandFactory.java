package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.GetClusterInfoResultCommand;
import cn.ttplatform.wh.support.AbstractMessageFactory;
import cn.ttplatform.wh.support.BufferPool;
import cn.ttplatform.wh.support.Message;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/4/28 10:54
 */
public class GetClusterInfoResultCommandFactory extends AbstractMessageFactory {

    private final Schema<GetClusterInfoResultCommand> schema = RuntimeSchema.getSchema(GetClusterInfoResultCommand.class);

    public GetClusterInfoResultCommandFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public Message create(byte[] content) {
        GetClusterInfoResultCommand message = new GetClusterInfoResultCommand();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        message.setCmd(content);
        return message;
    }

    @Override
    public byte[] getBytes(Message message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((GetClusterInfoResultCommand) message, schema, buffer);
    }
}
