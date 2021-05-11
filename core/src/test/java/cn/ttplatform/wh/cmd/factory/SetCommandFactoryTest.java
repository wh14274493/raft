package cn.ttplatform.wh.cmd.factory;

import cn.ttplatform.wh.cmd.SetCommand;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.support.FixedSizeLinkedBufferPool;
import cn.ttplatform.wh.support.Pool;
import io.protostuff.LinkedBuffer;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Wang Hao
 * @date 2021/5/11 0:29
 */
@Slf4j
public class SetCommandFactoryTest {

    SetCommandFactory factory;

    @Before
    public void setUp() {
        Pool<LinkedBuffer> pool = new FixedSizeLinkedBufferPool(10);
        factory = new SetCommandFactory(pool);
    }

    @Test
    public void getFactoryType() {
        Assert.assertEquals(DistributableType.SET_COMMAND, factory.getFactoryType());
    }

    @Test
    public void create() {
        StringBuilder value = new StringBuilder();
        while (value.length() < 256) {
            value.append(UUID.randomUUID());
        }
        String s = value.substring(0, 256);
        String id = UUID.randomUUID().toString();
        SetCommand setCommand = SetCommand.builder().id(id).key("wanghao").value(s).build();
        byte[] bytes = factory.getBytes(setCommand);
        long begin = System.nanoTime();
        IntStream.range(0, 10000).forEach(index -> factory.create(bytes, bytes.length));
        log.info("deserialize 10000 times cost {} ns.", System.nanoTime() - begin);
    }

    @Test
    public void testCreate() {
        StringBuilder value = new StringBuilder();
        while (value.length() < 256) {
            value.append(UUID.randomUUID());
        }
        String s = value.substring(0, 256);
        String id = UUID.randomUUID().toString();
        SetCommand setCommand = SetCommand.builder().id(id).key("wanghao").value(s).build();
        byte[] bytes = factory.getBytes(setCommand);
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bytes.length);
        byteBuffer.put(bytes);
        byteBuffer.flip();
        long begin = System.nanoTime();
        IntStream.range(0, 10000).forEach(index -> {
            factory.create(byteBuffer, bytes.length);
            byteBuffer.position(0);
        });
        log.info("deserialize 10000 times cost {} ns.", System.nanoTime() - begin);
    }

    @Test
    public void getBytes() {
        StringBuilder value = new StringBuilder();
        while (value.length() < 256) {
            value.append(UUID.randomUUID());
        }
        String s = value.substring(0, 256);
        String id = UUID.randomUUID().toString();
        SetCommand setCommand = SetCommand.builder().id(id).key("wanghao").value(s).build();
        long begin = System.nanoTime();
        IntStream.range(0, 10000).forEach(index -> factory.getBytes(setCommand));
        log.info("serialize 10000 times cost {} ns.", System.nanoTime() - begin);
    }
}