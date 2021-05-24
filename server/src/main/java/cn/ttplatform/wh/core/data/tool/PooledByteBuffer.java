package cn.ttplatform.wh.core.data.tool;

import cn.ttplatform.wh.support.Pool;
import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/5/9 11:22
 */
public class PooledByteBuffer {

    private final ByteBuffer buffer;
    private final Pool<PooledByteBuffer> pool;

    public PooledByteBuffer(ByteBuffer buffer, Pool<PooledByteBuffer> pool) {
        this.buffer = buffer;
        this.pool = pool;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void flip() {
        buffer.flip();
    }

    public void clear() {
        buffer.clear();
    }

    public int position() {
        return buffer.position();
    }

    public void position(int position) {
        buffer.position(position);
    }

    public int limit() {
        return buffer.limit();
    }

    public Buffer limit(int limit) {
        return this.buffer.limit(limit);
    }

    public int capacity() {
        return buffer.capacity();
    }

    public void putInt(int v) {
        buffer.putInt(v);
    }

    public int getInt() {
        return buffer.getInt();
    }

    public void putLong(long v) {
        buffer.putLong(v);
    }

    public long getLong() {
        return buffer.getLong();
    }

    public ByteBuffer get(byte[] bytes, int offset, int length) {
        return buffer.get(bytes, offset, length);
    }

    public ByteBuffer put(byte[] bytes) {
        return buffer.put(bytes);
    }

    public ByteBuffer put(byte[] bytes, int offset, int length) {
        return buffer.put(bytes, offset, length);
    }

    public void recycle() {
        pool.recycle(this);
    }
}
