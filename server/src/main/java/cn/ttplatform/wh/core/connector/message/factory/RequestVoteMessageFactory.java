package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.connector.message.RequestVoteMessage;
import cn.ttplatform.wh.support.AbstractDistributableFactory;
import cn.ttplatform.wh.support.Pool;
import cn.ttplatform.wh.support.Distributable;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:19
 */
public class RequestVoteMessageFactory extends AbstractDistributableFactory {

    private final Schema<RequestVoteMessage> schema = RuntimeSchema.getSchema(RequestVoteMessage.class);

    public RequestVoteMessageFactory(Pool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public int getFactoryType() {
        return DistributableType.REQUEST_VOTE;
    }

    @Override
    public Distributable create(byte[] content, int length) {
        RequestVoteMessage message = new RequestVoteMessage();
        ProtostuffIOUtil.mergeFrom(content, 0, length, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Distributable distributable, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((RequestVoteMessage) distributable, schema, buffer);
    }
}
