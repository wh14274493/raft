package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.support.AbstractDistributableFactory;
import cn.ttplatform.wh.support.BufferPool;
import cn.ttplatform.wh.core.connector.message.InstallSnapshotResultMessage;
import cn.ttplatform.wh.support.Distributable;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:14
 */
public class InstallSnapshotResultMessageFactory extends AbstractDistributableFactory {

    private final Schema<InstallSnapshotResultMessage> schema = RuntimeSchema
        .getSchema(InstallSnapshotResultMessage.class);

    public InstallSnapshotResultMessageFactory(BufferPool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public int getFactoryType() {
        return DistributableType.INSTALL_SNAPSHOT_RESULT;
    }

    @Override
    public Distributable create(byte[] content) {
        InstallSnapshotResultMessage message = new InstallSnapshotResultMessage();
        ProtostuffIOUtil.mergeFrom(content, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Distributable distributable, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((InstallSnapshotResultMessage) distributable, schema, buffer);
    }
}
