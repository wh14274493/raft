package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.core.BufferPool;
import cn.ttplatform.wh.core.common.AbstractMessageFactory;
import cn.ttplatform.wh.domain.message.InstallSnapshotResultMessage;
import cn.ttplatform.wh.domain.message.Message;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:14
 */
public class InstallSnapshotResultMessageFactory extends AbstractMessageFactory {

    private final Schema<InstallSnapshotResultMessage> schema = RuntimeSchema
        .getSchema(InstallSnapshotResultMessage.class);

    public InstallSnapshotResultMessageFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public Message create(byte[] content) {
        InstallSnapshotResultMessage message = new InstallSnapshotResultMessage();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Message message, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((InstallSnapshotResultMessage) message, schema, buffer);
    }
}
