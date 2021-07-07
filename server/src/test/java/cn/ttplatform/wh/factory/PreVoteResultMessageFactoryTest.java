package cn.ttplatform.wh.factory;

import cn.ttplatform.wh.message.factory.PreVoteResultMessageFactory;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.message.PreVoteResultMessage;
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
 * @date 2021/5/11 23:00
 */
@Slf4j
public class PreVoteResultMessageFactoryTest {

    PreVoteResultMessageFactory factory;

    @Before
    public void setUp() throws Exception {
        Pool<LinkedBuffer> pool = new FixedSizeLinkedBufferPool(10);
        factory = new PreVoteResultMessageFactory(pool);
    }

    @Test
    public void getFactoryType() {
        Assert.assertEquals(DistributableType.PRE_VOTE_RESULT, factory.getFactoryType());
    }

    @Test
    public void create() {
        PreVoteResultMessage message = PreVoteResultMessage.builder()
            .term(0).isVoted(true).sourceId("A").build();
        byte[] bytes = factory.getBytes(message);
        long begin = System.nanoTime();
        IntStream.range(0, 10000).forEach(index -> factory.create(bytes, bytes.length));
        log.info("deserialize 10000 times cost {} ns.", System.nanoTime() - begin);
    }

    @Test
    public void testCreate() {
        PreVoteResultMessage message = PreVoteResultMessage.builder()
            .term(0).isVoted(true).sourceId("A").build();
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
        PreVoteResultMessage message = PreVoteResultMessage.builder()
            .term(0).isVoted(true).sourceId("A").build();
        long begin = System.nanoTime();
        IntStream.range(0, 10000).forEach(index -> factory.getBytes(message));
        log.info("serialize 10000 times cost {} ns.", System.nanoTime() - begin);
    }

    @Test
    public void testGetBytes() {
        PreVoteResultMessage message = PreVoteResultMessage.builder()
            .term(0).isVoted(true).sourceId("A").build();
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