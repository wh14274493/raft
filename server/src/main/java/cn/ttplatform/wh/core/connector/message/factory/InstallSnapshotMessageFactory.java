package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.core.connector.message.InstallSnapshotMessage;
import cn.ttplatform.wh.support.AbstractDistributableFactory;
import cn.ttplatform.wh.support.Pool;
import cn.ttplatform.wh.support.Distributable;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:11
 */
public class InstallSnapshotMessageFactory extends AbstractDistributableFactory {

    private final Schema<InstallSnapshotMessage> schema = RuntimeSchema.getSchema(InstallSnapshotMessage.class);

    public InstallSnapshotMessageFactory(Pool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public int getFactoryType() {
        return DistributableType.INSTALL_SNAPSHOT;
    }

    @Override
    public Distributable create(byte[] content, int length) {
        InstallSnapshotMessage message = new InstallSnapshotMessage();
        ProtostuffIOUtil.mergeFrom(content, 0, length, message, schema);
        return message;
    }

    @Override
    public byte[] getBytes(Distributable distributable, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((InstallSnapshotMessage) distributable, schema, buffer);
    }
}
