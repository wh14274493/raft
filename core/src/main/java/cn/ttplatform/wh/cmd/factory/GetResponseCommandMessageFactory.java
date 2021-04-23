package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.GetResponseCommand;
import cn.ttplatform.wh.cmd.Message;
import cn.ttplatform.wh.support.AbstractMessageFactory;
import cn.ttplatform.wh.support.BufferPool;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/4/16 22:49
 */
public class GetResponseCommandMessageFactory extends AbstractMessageFactory {
    private final Schema<GetResponseCommand> schema = RuntimeSchema.getSchema(GetResponseCommand.class);

    public GetResponseCommandMessageFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public Message create(byte[] content) {
        GetResponseCommand message = new GetResponseCommand();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Message message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((GetResponseCommand) message, schema, buffer);
    }
}
