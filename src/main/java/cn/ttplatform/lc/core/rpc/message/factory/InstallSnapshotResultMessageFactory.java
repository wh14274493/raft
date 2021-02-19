package cn.ttplatform.lc.core.rpc.message.factory;

import cn.ttplatform.lc.core.rpc.message.domain.InstallSnapshotResultMessage;
import cn.ttplatform.lc.core.rpc.message.domain.Message;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:14
 */
public class InstallSnapshotResultMessageFactory implements MessageFactory {

    private final Schema<InstallSnapshotResultMessage> schema = RuntimeSchema
        .getSchema(InstallSnapshotResultMessage.class);

    @Override
    public Message create(byte[] content) {
        InstallSnapshotResultMessage message = new InstallSnapshotResultMessage();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(LinkedBuffer buffer, Message message) {
        return ProtostuffIOUtil.toByteArray((InstallSnapshotResultMessage) message, schema, buffer);
    }
}
