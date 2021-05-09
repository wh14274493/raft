package cn.ttplatform.wh.support;

import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * @author Wang Hao
 * @date 2021/4/21 13:07
 */
public abstract class AbstractPool<T> implements Pool<T> {

    protected final int bufferSizeLimit;
    protected final int poolSize;
    protected final TreeMap<Integer, T> pool;

    protected AbstractPool(int poolSize, int bufferSizeLimit) {
        this.bufferSizeLimit = bufferSizeLimit;
        this.poolSize = poolSize;
        this.pool = new TreeMap<>();
    }

    @Override
    public T allocate(int size) {
        if (!pool.isEmpty()) {
            Entry<Integer, T> bufferEntry = pool.ceilingEntry(size);
            if (bufferEntry != null) {
                return pool.remove(bufferEntry.getKey());
            }
        }
        return doAllocate(size);
    }

    public abstract T doAllocate(int size);
}
