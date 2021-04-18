package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.core.common.BufferPool;
import cn.ttplatform.wh.core.common.AbstractMessageFactory;
import cn.ttplatform.wh.domain.message.InstallSnapshotMessage;
import cn.ttplatform.wh.domain.message.Message;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:11
 */
public class InstallSnapshotMessageFactory extends AbstractMessageFactory {

    private final Schema<InstallSnapshotMessage> schema = RuntimeSchema.getSchema(InstallSnapshotMessage.class);

    public InstallSnapshotMessageFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public Message create(byte[] content) {
        InstallSnapshotMessage message = new InstallSnapshotMessage();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Message message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((InstallSnapshotMessage) message, schema, buffer);
    }
}
