package cn.ttplatform.wh.support;

/**
 * @author Wang Hao
 * @date 2021/5/4 14:15
 */
public class ByteArrayPool extends AbstractPool<byte[]> {

    public ByteArrayPool(int poolSize, int bufferSizeLimit) {
        super(poolSize, bufferSizeLimit);
    }

    @Override
    public byte[] doAllocate(int size) {
        return new byte[size];
    }

    @Override
    public void recycle(byte[] buffer) {
        if (pool.size() < poolSize && buffer.length <= bufferSizeLimit) {
            pool.put(buffer.length, buffer);
        }
    }
}
