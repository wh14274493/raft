package cn.ttplatform.wh.message.factory;

import cn.ttplatform.wh.message.InstallSnapshotResultMessage;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.constant.ErrorMessage;
import cn.ttplatform.wh.exception.MessageParseException;
import cn.ttplatform.wh.support.AbstractDistributableFactory;
import cn.ttplatform.wh.support.Distributable;
import cn.ttplatform.wh.support.Pool;
import io.protostuff.ByteBufferInput;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/2/18 16:14
 */
public class InstallSnapshotResultMessageFactory extends AbstractDistributableFactory {

    private final Schema<InstallSnapshotResultMessage> schema = RuntimeSchema
        .getSchema(InstallSnapshotResultMessage.class);

    public InstallSnapshotResultMessageFactory(Pool<LinkedBuffer> pool) {
        super(pool);
    }

    @Override
    public int getFactoryType() {
        return DistributableType.INSTALL_SNAPSHOT_RESULT;
    }

    @Override
    public Distributable create(byte[] content, int length) {
        InstallSnapshotResultMessage message = new InstallSnapshotResultMessage();
        ProtostuffIOUtil.mergeFrom(content, 0, length, message, schema);
        return message;
    }

    @Override
    public Distributable create(ByteBuffer byteBuffer) {
        InstallSnapshotResultMessage message = new InstallSnapshotResultMessage();
        try {
            schema.mergeFrom(new ByteBufferInput(byteBuffer, true), message);
        } catch (IOException e) {
            throw new MessageParseException(ErrorMessage.MESSAGE_PARSE_ERROR);
        }
        return message;
    }

    @Override
    public byte[] getBytes(Distributable distributable, LinkedBuffer buffer) {
        return ProtostuffIOUtil.toByteArray((InstallSnapshotResultMessage) distributable, schema, buffer);
    }

    @Override
    public void getBytes(Distributable distributable, LinkedBuffer buffer, OutputStream outputStream)
        throws IOException {
        ProtostuffIOUtil.writeTo(outputStream, (InstallSnapshotResultMessage) distributable, schema, buffer);
    }
}
