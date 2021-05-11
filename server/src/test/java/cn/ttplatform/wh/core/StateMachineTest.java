package cn.ttplatform.wh.core;

import cn.ttplatform.wh.support.FixedSizeLinkedBufferPool;
import cn.ttplatform.wh.support.Pool;
import io.protostuff.LinkedBuffer;
import io.protostuff.MessageMapSchema;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.runtime.RuntimeSchema;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Wang Hao
 * @date 2021/5/11 19:21
 */
@Slf4j
public class StateMachineTest {

    StateMachine stateMachine;
    Pool<LinkedBuffer> pool;

    @Before
    public void setUp() throws Exception {
        pool = new FixedSizeLinkedBufferPool(10);
        stateMachine = new StateMachine(pool);
    }

    @Test
    public void get() {
        stateMachine.set("key", "value");
        String value = stateMachine.get("key");
        Assert.assertEquals("value", value);
    }

    @Test
    public void set() {
        int count = ThreadLocalRandom.current().nextInt(1000000);
        long begin = System.nanoTime();
        IntStream.range(0, count).forEach(index -> {
            stateMachine.set("key" + index, "value");
        });
        log.info("set {} times cost {} ns.", count, System.nanoTime() - begin);
        Assert.assertEquals(count, stateMachine.getPairs());
    }

    @Test
    public void getLastApplied() {
        int count = ThreadLocalRandom.current().nextInt(1000000);
        stateMachine.setLastApplied(count);
        Assert.assertEquals(count, stateMachine.getLastApplied());
    }

    @Test
    public void generateSnapshotData() {
        int count = ThreadLocalRandom.current().nextInt(1000000);
        IntStream.range(0, count).forEach(index -> {
            stateMachine.set("key" + index, "value");
        });
        log.info("set {} pairs.", count);
        long begin = System.nanoTime();
        byte[] bytes = stateMachine.generateSnapshotData();
        log.info("generate {} bytes snapshot cost {} ns.", bytes.length, System.nanoTime() - begin);
    }

    @Test
    public void testMap() {
        MessageMapSchema<String, String> mapSchema = new MessageMapSchema<>(
            RuntimeSchema.getSchema(String.class), RuntimeSchema.getSchema(String.class));
        int count = ThreadLocalRandom.current().nextInt(1000000);
        Map<String, String> map = new HashMap<>();
        IntStream.range(0, count).forEach(index -> {
            map.put("key" + index, "value");
        });
        log.info("set {} pairs.", count);
        long begin = System.nanoTime();
        byte[] bytes = ProtostuffIOUtil.toByteArray(map, mapSchema, LinkedBuffer.allocate());
        log.info("serialize {} bytes map cost {} ns.", bytes.length, System.nanoTime() - begin);
        Map<String, String> newMap = new HashMap<>();
        begin = System.nanoTime();
        ProtostuffIOUtil.mergeFrom(bytes, newMap, mapSchema);
        log.info("map size is {}.", newMap.size());
        log.info("deserialize {} bytes snapshot cost {} ns.", bytes.length, System.nanoTime() - begin);
    }

    @Test
    public void applySnapshotData() {
    }
}