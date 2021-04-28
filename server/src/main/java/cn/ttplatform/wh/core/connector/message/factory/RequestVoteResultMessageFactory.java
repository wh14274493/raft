package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.connector.message.RequestVoteResultMessage;
import cn.ttplatform.wh.support.AbstractDistributableFactory;
import cn.ttplatform.wh.support.BufferPool;
import cn.ttplatform.wh.support.Distributable;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:21
 */
public class RequestVoteResultMessageFactory extends AbstractDistributableFactory {

    private final Schema<RequestVoteResultMessage> schema = RuntimeSchema.getSchema(RequestVoteResultMessage.class);

    public RequestVoteResultMessageFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public int getFactoryType() {
        return DistributableType.REQUEST_VOTE_RESULT;
    }

    @Override
    public Distributable create(byte[] content) {
        RequestVoteResultMessage message = new RequestVoteResultMessage();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Distributable distributable, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((RequestVoteResultMessage) distributable, schema, buffer);
    }
}
