package cn.ttplatform.wh.core.connector.message.factory;

import cn.ttplatform.wh.message.factory.InstallSnapshotResultMessageFactory;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.message.InstallSnapshotResultMessage;
import cn.ttplatform.wh.support.FixedSizeLinkedBufferPool;
import cn.ttplatform.wh.support.Pool;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.protostuff.LinkedBuffer;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Wang Hao
 * @date 2021/5/11 22:56
 */
@Slf4j
public class InstallSnapshotResultMessageFactoryTest {

    InstallSnapshotResultMessageFactory factory;

    @Before
    public void setUp() throws Exception {
        Pool<LinkedBuffer> pool = new FixedSizeLinkedBufferPool(10);
        factory = new InstallSnapshotResultMessageFactory(pool);
    }

    @Test
    public void getFactoryType() {
        Assert.assertEquals(DistributableType.INSTALL_SNAPSHOT_RESULT, factory.getFactoryType());
    }

    @Test
    public void create() {
        InstallSnapshotResultMessage message = InstallSnapshotResultMessage.builder()
            .offset(0).term(0).sourceId("A").success(true).done(true).build();
        byte[] bytes = factory.getBytes(message);
        long begin = System.nanoTime();
        IntStream.range(0, 10000).forEach(index -> factory.create(bytes, bytes.length));
        log.info("deserialize 10000 times cost {} ns.", System.nanoTime() - begin);
    }

    @Test
    public void testCreate() {
        InstallSnapshotResultMessage message = InstallSnapshotResultMessage.builder()
            .offset(0).term(0).sourceId("A").success(true).done(true).build();
        byte[] bytes = factory.getBytes(message);
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
        InstallSnapshotResultMessage message = InstallSnapshotResultMessage.builder()
            .offset(0).term(0).sourceId("A").success(true).done(true).build();
        long begin = System.nanoTime();
        IntStream.range(0, 10000).forEach(index -> factory.getBytes(message));
        log.info("serialize 10000 times cost {} ns.", System.nanoTime() - begin);
    }

    @Test
    public void testGetBytes() {
        InstallSnapshotResultMessage message = InstallSnapshotResultMessage.builder()
            .offset(0).term(0).sourceId("A").success(true).done(true).build();
        UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(true);
        ByteBuf byteBuf = allocator.directBuffer();
        long begin = System.nanoTime();
        IntStream.range(0, 10000).forEach(index -> {
            factory.getBytes(message, byteBuf);
            byteBuf.clear();
        });
        log.info("serialize 10000 times cost {} ns.", System.nanoTime() - begin);
    }
}