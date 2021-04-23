package cn.ttplatform.wh.core.support;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Wang Hao
 * @date 2021/4/23 11:07
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> {

    private final int capacity;

    public LRUCache(int capacity) {
        super(capacity, 0.75F, true);
        this.capacity = capacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > capacity;
    }

}
