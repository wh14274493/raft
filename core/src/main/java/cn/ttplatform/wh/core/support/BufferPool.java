package cn.ttplatform.wh.core.support;

/**
 * @author Wang Hao
 * @date 2021/2/19 19:36
 */
public interface BufferPool<T> {

    /**
     * apply to get a fixed size buffer from pool
     *
     * @param size buffer size
     * @return buffer
     */
    default T allocate(int size) {
        throw new UnsupportedOperationException();
    }

    /**
     * apply to get a fixed size buffer from pool
     *
     * @return buffer
     */
    default T allocate() {
        throw new UnsupportedOperationException();
    }

    /**
     * recycle a buffer, note that need to init it before reusing
     *
     * @param buffer a buffer can be reused
     */
    void recycle(T buffer);
}
