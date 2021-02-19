package cn.ttplatform.lc.core.rpc.message.factory;

import cn.ttplatform.lc.core.rpc.message.domain.InstallSnapshotMessage;
import cn.ttplatform.lc.core.rpc.message.domain.Message;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:11
 */
public class InstallSnapshotMessageFactory implements MessageFactory {

    private final Schema<InstallSnapshotMessage> schema = RuntimeSchema.getSchema(InstallSnapshotMessage.class);

    @Override
    public Message create(byte[] content) {
        InstallSnapshotMessage message = new InstallSnapshotMessage();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(LinkedBuffer buffer, Message message) {
        return ProtostuffIOUtil.toByteArray((InstallSnapshotMessage) message, schema, buffer);
    }
}
