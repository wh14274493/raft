package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.support.Message;
import cn.ttplatform.wh.cmd.RedirectCommand;
import cn.ttplatform.wh.support.AbstractMessageFactory;
import cn.ttplatform.wh.support.BufferPool;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/4/23 23:18
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
