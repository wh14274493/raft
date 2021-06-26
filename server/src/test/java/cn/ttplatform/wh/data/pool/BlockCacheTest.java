package cn.ttplatform.wh.data.pool;

import cn.ttplatform.wh.data.FileConstant;
import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;

import java.io.File;
import java.nio.ByteBuffer;

@Slf4j
public class BlockCacheTest extends TestCase {

    BlockCache blockCache;

    public void setUp() throws Exception {
        File block = File.createTempFile("temp-", "block");
        blockCache = new BlockCache(50, 1024 * 1024, 1000L, block, FileConstant.LOG_FILE_HEADER_SIZE);
    }

    public void tearDown() throws Exception {
        blockCache.close();
    }

    public void testAppendBlock() {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024 * 1024);
        byteBuffer.limit(byteBuffer.capacity());
        byteBuffer.position(0);
        long begin = System.nanoTime();
        blockCache.appendBlock(byteBuffer);
        log.info("append a block cost {} ns.", System.nanoTime() - begin);
    }

    public void testAppendByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024 * 1024 * 10);
        byteBuffer.position(byteBuffer.capacity());
        long begin = System.nanoTime();
        blockCache.appendByteBuffer(byteBuffer);
        log.info("append a byteBuffer[{}] cost {} ns.", byteBuffer, System.nanoTime() - begin);
    }

    public void testAppendBytes() {
        byte[] bytes = new byte[1027 * 1024 * 10];
        long begin = System.nanoTime();
        blockCache.appendBytes(bytes);
        log.info("append {} bytes cost {} ns.", bytes.length, System.nanoTime() - begin);
    }

    public void testAppendInt() {
        long begin = System.nanoTime();
        blockCache.appendInt(1);
        log.info("append a int cost {} ns.", System.nanoTime() - begin);
    }

    public void testAppendLong() {
        long begin = System.nanoTime();
        blockCache.appendLong(1L);
        log.info("append a long cost {} ns.", System.nanoTime() - begin);
    }

    public void testGetInt() {
        testAppendInt();
        long begin = System.nanoTime();
        int anInt = blockCache.getInt(8);
        log.info("get a int cost {} ns.", System.nanoTime() - begin);
        Assert.assertEquals(1, anInt);
    }

    public void testGetLong() {
        testAppendLong();
        long begin = System.nanoTime();
        long anLong = blockCache.getLong(8);
        log.info("get a long cost {} ns.", System.nanoTime() - begin);
        Assert.assertEquals(1L, anLong);
    }

}