package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.connector.message.AppendLogEntriesMessage;
import cn.ttplatform.wh.support.AbstractDistributableFactory;
import cn.ttplatform.wh.support.Pool;
import cn.ttplatform.wh.support.Distributable;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 11:27
 */
public class AppendLogEntriesMessageFactory extends AbstractDistributableFactory {

    private final Schema<AppendLogEntriesMessage> schema = RuntimeSchema.getSchema(AppendLogEntriesMessage.class);

    public AppendLogEntriesMessageFactory(Pool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public int getFactoryType() {
        return DistributableType.APPEND_LOG_ENTRIES;
    }

    @Override
    public Distributable create(byte[] content, int length) {
        AppendLogEntriesMessage message = new AppendLogEntriesMessage();
        ProtostuffIOUtil.mergeFrom(content, 0, length, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Distributable distributable, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((AppendLogEntriesMessage) distributable, schema, buffer);
    }

}
