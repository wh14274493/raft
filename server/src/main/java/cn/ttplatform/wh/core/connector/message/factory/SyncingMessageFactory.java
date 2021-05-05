package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.connector.message.SyncingMessage;
import cn.ttplatform.wh.support.AbstractDistributableFactory;
import cn.ttplatform.wh.support.Pool;
import cn.ttplatform.wh.support.Distributable;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/5/3 11:14
 */
public class SyncingMessageFactory extends AbstractDistributableFactory {

    private final Schema<SyncingMessage> schema = RuntimeSchema.getSchema(SyncingMessage.class);

    public SyncingMessageFactory(Pool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public byte[] getBytes(Distributable distributable, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((SyncingMessage) distributable, schema, buffer);
    }

    @Override
    public int getFactoryType() {
        return DistributableType.SYNCING;
    }

    @Override
    public Distributable create(byte[] content, int length) {
        SyncingMessage message = new SyncingMessage();
        ProtostuffIOUtil.mergeFrom(content, 0, length, message, schema);
        return message;
    }
}
