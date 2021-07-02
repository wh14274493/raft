package cn.ttplatform.wh;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

/**
 * @author Wang Hao
 * @date 2021/6/4 14:44
 */
@Slf4j
public class CollectionTest {

    @Test
    public void testLinkedList() {
        LinkedList<Integer> integers = new LinkedList<>();
        int count = 100000;
        long begin = System.nanoTime();
        IntStream.range(0, count).forEach(index -> integers.add(1));
        log.info("LinkedList add {} element cost {} ns.", count, System.nanoTime() - begin);
        begin = System.nanoTime();
        IntStream.range(0, count).forEach(index -> integers.get(index));
        log.info("LinkedList get {} element cost {} ns.", count, System.nanoTime() - begin);
        begin = System.nanoTime();
        integers.subList(10000, 90000);
        log.info("LinkedList sublist from {} to {} cost {} ns.", 10000, 90000, System.nanoTime() - begin);
        begin = System.nanoTime();
        while (!integers.isEmpty()) {
            integers.pollLast();
        }
        log.info("LinkedList remove {} element from last cost {} ns.", count, System.nanoTime() - begin);
    }

    @Test
    public void testArrayList() {
        ArrayList<Integer> integers = new ArrayList<>();
        int count = 100000;
        long begin = System.nanoTime();
        IntStream.range(0, count).forEach(index -> integers.add(1));
        log.info("ArrayList add {} element cost {} ns.", count, System.nanoTime() - begin);
        begin = System.nanoTime();
        IntStream.range(0, count).forEach(index -> integers.get(index));
        log.info("ArrayList get {} element cost {} ns.", count, System.nanoTime() - begin);
        begin = System.nanoTime();
        integers.subList(10000, 90000);
        log.info("ArrayList sublist from {} to {} cost {} ns.", 10000, 90000, System.nanoTime() - begin);
        ArrayList<Integer> clone = (ArrayList<Integer>) integers.clone();
        begin = System.nanoTime();
        while (!integers.isEmpty()) {
            integers.remove(0);
        }
        log.info("ArrayList remove {} element from first cost {} ns.", count, System.nanoTime() - begin);
        begin = System.nanoTime();
        while (!integers.isEmpty()) {
            clone.remove(integers.size() - 1);
        }
        log.info("ArrayList remove {} element from last cost {} ns.", count, System.nanoTime() - begin);
    }

    @Test
    public void testLinkedHashMap() {
        LinkedHashMap<Integer, Integer> map = new LinkedHashMap<>();
        int count = 100000;
        long begin = System.nanoTime();
        IntStream.range(0, count).forEach(index -> map.put(index, index));
        log.info("LinkedHashMap add {} element cost {} ns.", count, System.nanoTime() - begin);
        begin = System.nanoTime();
        IntStream.range(0, count).forEach(index -> map.get(index));
        log.info("LinkedHashMap get {} element cost {} ns.", count, System.nanoTime() - begin);
        begin = System.nanoTime();
        IntStream.range(0, count).forEach(index -> map.remove(index));
        log.info("LinkedHashMap remove {} element cost {} ns.", count, System.nanoTime() - begin);
    }

    @Test
    public void testTreeMap() {
        TreeMap<Integer, Integer> map = new TreeMap<>();
        int count = 100000;
        long begin = System.nanoTime();
        IntStream.range(0, count).forEach(index -> map.put(index, index));
        log.info("TreeMap add {} element cost {} ns.", count, System.nanoTime() - begin);
        begin = System.nanoTime();
        IntStream.range(0, count).forEach(index -> map.get(index));
        log.info("TreeMap get {} element cost {} ns.", count, System.nanoTime() - begin);
        begin = System.nanoTime();
        IntStream.range(0, count).forEach(index -> map.remove(index));
        log.info("TreeMap remove {} element cost {} ns.", count, System.nanoTime() - begin);
        begin = System.nanoTime();
        map.subMap(10000, 90000);
        log.info("TreeMap subMap from {} to {} cost {} ns.", 10000, 90000, System.nanoTime() - begin);
    }
}
