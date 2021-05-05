package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.connector.message.AppendLogEntriesResultMessage;
import cn.ttplatform.wh.support.AbstractDistributableFactory;
import cn.ttplatform.wh.support.Pool;
import cn.ttplatform.wh.support.Distributable;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:09
 */
public class AppendLogEntriesResultMessageFactory extends AbstractDistributableFactory {

    private final Schema<AppendLogEntriesResultMessage> schema = RuntimeSchema
        .getSchema(AppendLogEntriesResultMessage.class);

    public AppendLogEntriesResultMessageFactory(Pool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public int getFactoryType() {
        return DistributableType.APPEND_LOG_ENTRIES_RESULT;
    }

    @Override
    public Distributable create(byte[] content, int length) {
        AppendLogEntriesResultMessage message = new AppendLogEntriesResultMessage();
        ProtostuffIOUtil.mergeFrom(content, 0, length, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Distributable distributable, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((AppendLogEntriesResultMessage) distributable, schema, buffer);
    }
}
