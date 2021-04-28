package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.GetClusterInfoCommand;
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
public class GetClusterInfoCommandFactory extends AbstractMessageFactory {

    private final Schema<GetClusterInfoCommand> schema = RuntimeSchema.getSchema(GetClusterInfoCommand.class);

    public GetClusterInfoCommandFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public Message create(byte[] content) {
        GetClusterInfoCommand message = new GetClusterInfoCommand();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        message.setCmd(content);
        return message;
    }

    @Override
    public byte[] getBytes(Message message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((GetClusterInfoCommand) message, schema, buffer);
    }
}
