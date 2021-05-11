package cn.ttplatform.wh.support;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.protostuff.LinkedBuffer;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/3/15 14:25
 */
public abstract class AbstractDistributableFactory implements DistributableFactory {

    Pool<LinkedBuffer> pool;

    protected AbstractDistributableFactory(Pool<LinkedBuffer> pool) {
        this.pool = pool;
    }

    @Override
    public byte[] getBytes(Distributable distributable) {
        LinkedBuffer buffer = pool.allocate();
        try {
            return getBytes(distributable, buffer);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            pool.recycle(buffer);
        }
    }

    public abstract byte[] getBytes(Distributable distributable, LinkedBuffer buffer);

    @Override
    public void getBytes(Distributable distributable, ByteBuf byteBuffer) {
        byteBuffer.writeInt(getFactoryType());
        int writerIndex = byteBuffer.writerIndex();
        byteBuffer.writeInt(getFactoryType());
        ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(byteBuffer);
        LinkedBuffer buffer = pool.allocate();
        try {
            getBytes(distributable, buffer, byteBuffer, byteBufOutputStream);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pool.recycle(buffer);
        }
        int newWriterIndex = byteBuffer.writerIndex();
        byteBuffer.writerIndex(writerIndex);
        byteBuffer.writeInt(newWriterIndex - 8);
        byteBuffer.writerIndex(newWriterIndex);
    }

    public abstract void getBytes(Distributable distributable, LinkedBuffer buffer, ByteBuf byteBuffer,
        OutputStream outputStream) throws IOException;


    @Override
    public Distributable create(ByteBuffer byteBuffer, int contentLength) {
        int limit = byteBuffer.limit();
        try {
            int position = byteBuffer.position();
            byteBuffer.limit(position + contentLength);
            return create(byteBuffer);
        } finally {
            byteBuffer.limit(limit);
        }
    }

    public abstract Distributable create(ByteBuffer byteBuffer);
}
