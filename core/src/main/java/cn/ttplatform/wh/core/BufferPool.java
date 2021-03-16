package cn.ttplatform.wh.core;

/**
 * @author Wang Hao
 * @date 2021/2/19 19:36
 */
public interface BufferPool<T> {

    /**
     * apply to get a buffer from pool
     *
     * @return buffer
     */
    T allocate();

    /**
     * recycle a buffer, note that need to init it before reusing
     *
     * @param buffer a buffer can be reused
     */
    void recycle(T buffer);
}
