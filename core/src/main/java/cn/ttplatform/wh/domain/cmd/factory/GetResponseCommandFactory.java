package cn.ttplatform.wh.domain.cmd.factory;

import cn.ttplatform.wh.core.common.AbstractMessageFactory;
import cn.ttplatform.wh.core.common.BufferPool;
import cn.ttplatform.wh.domain.cmd.GetResponseCommand;
import cn.ttplatform.wh.domain.message.Message;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/4/16 22:49
 */
public class GetResponseCommandFactory extends AbstractMessageFactory {
    private final Schema<GetResponseCommand> schema = RuntimeSchema.getSchema(GetResponseCommand.class);

    public GetResponseCommandFactory(BufferPool<LinkedBuffer> pool) {
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
