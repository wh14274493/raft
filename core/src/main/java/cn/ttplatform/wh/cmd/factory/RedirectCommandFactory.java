package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.RedirectCommand;
import cn.ttplatform.wh.core.connector.message.Message;
import cn.ttplatform.wh.core.support.AbstractMessageFactory;
import cn.ttplatform.wh.core.support.BufferPool;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/4/20 12:53
 */
public class RedirectCommandFactory extends AbstractMessageFactory {

    private final Schema<RedirectCommand> schema = RuntimeSchema.getSchema(RedirectCommand.class);

    public RedirectCommandFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public byte[] getBytes(Message message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((RedirectCommand) message, schema, buffer);
    }

    @Override
    public Message create(byte[] content) {
        RedirectCommand message = new RedirectCommand();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }
}
