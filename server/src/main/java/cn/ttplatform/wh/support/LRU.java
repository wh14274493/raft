package cn.ttplatform.wh.support;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Wang Hao
 * @date 2021/4/23 11:07
 */
public class LRU<K, V> {

    final int capacity;
    protected int used;
    protected final Map<K, KVEntry<K, V>> cache;
    private final KVEntry<K, V> head;
    private KVEntry<K, V> tail;

    public LRU(int capacity) {
        this.capacity = capacity;
        this.cache = new HashMap<>((int) (capacity / 0.75f) + 1);
        // head is a Sentinel node
        this.head = new KVEntry<>();
        this.tail = head;
    }

    public LRU(int capacity, Map<K, KVEntry<K, V>> cache) {
        this.cache = cache;
        this.capacity = capacity;
        // head is a Sentinel node
        this.head = new KVEntry<>();
        this.tail = head;
    }

    public V get(K key) {
        KVEntry<K, V> kvEntry = getEntry(key);
        return kvEntry == null ? null : kvEntry.value;
    }

    private KVEntry<K, V> getEntry(K key) {
        KVEntry<K, V> kvEntry = cache.get(key);
        if (kvEntry == null) {
            return null;
        }
        if (kvEntry != tail) {
            KVEntry<K, V> pre = kvEntry.pre;
            pre.next = kvEntry.next;
            kvEntry.next.pre = pre;
            tail.next = kvEntry;
            kvEntry.pre = tail;
            kvEntry.next = null;
            tail = tail.next;
        }
        return kvEntry;
    }

    public KVEntry<K, V> put(K key, V value) {
        KVEntry<K, V> old = null;
        KVEntry<K, V> kvEntry = getEntry(key);
        if (kvEntry == null) {
            old = removeFirstIfNeeded();
            kvEntry = new KVEntry<>(key, value);
            cache.put(key, kvEntry);
            tail.next = kvEntry;
            kvEntry.pre = tail;
            tail = tail.next;
            used++;
        } else {
            kvEntry.value = value;
        }
        return old;
    }

    private KVEntry<K, V> removeFirstIfNeeded() {
        KVEntry<K, V> kvEntry = null;
        if (used == capacity) {
            kvEntry = cache.remove(head.next.key);
            if (head.next == tail) {
                head.next = null;
                tail = head;
            } else {
                head.next = head.next.next;
                head.next.pre = head;
            }
            used--;
        }
        return kvEntry;
    }

    public void clear() {
        cache.clear();
        head.next.pre = null;
        head.next = null;
        tail = head;
        used = 0;
    }

    public KVEntry<K, V> remove(K key) {
        KVEntry<K, V> kvEntry = cache.get(key);
        if (kvEntry != null) {
            remove(kvEntry);
        }
        return kvEntry;
    }

    protected void remove(KVEntry<K, V> kvEntry) {
        if (kvEntry == tail) {
            tail = kvEntry.pre;
            kvEntry.pre = null;
            tail.next = null;
        } else {
            KVEntry<K, V> next = kvEntry.next;
            KVEntry<K, V> pre = kvEntry.pre;
            pre.next = next;
            next.pre = pre;
            kvEntry.pre = null;
            kvEntry.next = null;
        }
    }

    @Override
    public String toString() {
        return "LRU{" +
                "capacity=" + capacity +
                ", used=" + used +
                '}';
    }

    @Getter
    public static class KVEntry<K, V> {

        KVEntry<K, V> pre;
        KVEntry<K, V> next;
        K key;
        V value;

        public KVEntry() {
        }

        public KVEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

}
