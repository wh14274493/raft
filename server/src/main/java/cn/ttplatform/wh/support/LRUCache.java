package cn.ttplatform.wh.support;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;

/**
 * @author Wang Hao
 * @date 2021/4/23 11:07
 */
public class LRUCache<K, V> {


    final int capacity;
    protected int size;
    protected final Map<K, Entry<K, V>> cache;
    private final Entry<K, V> head;
    private Entry<K, V> tail;

    public LRUCache(int capacity) {
        this.capacity = capacity;
        this.cache = new HashMap<>((int) (capacity / 0.75f) + 1);
        // head is a Sentinel node
        this.head = new Entry<>();
        this.tail = head;
    }

    public V get(K key) {
        Entry<K, V> entry = getEntry(key);
        return entry == null ? null : entry.value;
    }

    private Entry<K, V> getEntry(K key) {
        Entry<K, V> entry = cache.get(key);
        if (entry == null) {
            return null;
        }
        if (entry != tail) {
            Entry<K, V> pre = entry.pre;
            pre.next = entry.next;
            entry.next.pre = pre;
            tail.next = entry;
            entry.pre = tail;
            entry.next = null;
            tail = tail.next;
        }
        return entry;
    }

    public void put(K key, V value) {
        Entry<K, V> entry = getEntry(key);
        if (entry == null) {
            removeFirst();
            entry = new Entry<>(key, value);
            cache.put(key, entry);
            tail.next = entry;
            entry.pre = tail;
            tail = tail.next;
            size++;
        } else {
            entry.value = value;
        }
    }

    private void removeFirst() {
        if (size == capacity) {
            cache.remove(head.next.key);
            if (head.next == tail) {
                head.next = null;
                tail = head;
            } else {
                head.next = head.next.next;
                head.next.pre = head;
            }
            size--;
        }
    }

    public void clear() {
        cache.clear();
        head.next.pre = null;
        head.next = null;
        size = 0;
    }

    protected void remove(Entry<K, V> entry) {
        Entry<K, V> next = entry.next;
        Entry<K, V> pre = entry.pre;
        pre.next = next;
        if (next != null) {
            next.pre = pre;
        }
        entry.pre = null;
        entry.next = null;
    }

    @Getter
    public static class Entry<K, V> {

        Entry<K, V> pre;
        Entry<K, V> next;
        K key;
        V value;

        public Entry() {
        }

        public Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

}
