package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.connector.message.PreVoteResultMessage;
import cn.ttplatform.wh.support.AbstractDistributableFactory;
import cn.ttplatform.wh.support.Pool;
import cn.ttplatform.wh.support.Distributable;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:18
 */
public class PreVoteResultMessageFactory extends AbstractDistributableFactory {

    private final Schema<PreVoteResultMessage> schema = RuntimeSchema.getSchema(PreVoteResultMessage.class);

    public PreVoteResultMessageFactory(Pool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public int getFactoryType() {
        return DistributableType.PRE_VOTE_RESULT;
    }

    @Override
    public Distributable create(byte[] content, int length) {
        PreVoteResultMessage message = new PreVoteResultMessage();
        ProtostuffIOUtil.mergeFrom(content, 0, length, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Distributable distributable, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((PreVoteResultMessage) distributable, schema, buffer);
    }
}
