package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.support.AbstractMessageFactory;
import cn.ttplatform.wh.support.BufferPool;
import cn.ttplatform.wh.core.connector.message.InstallSnapshotResultMessage;
import cn.ttplatform.wh.cmd.Message;
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
