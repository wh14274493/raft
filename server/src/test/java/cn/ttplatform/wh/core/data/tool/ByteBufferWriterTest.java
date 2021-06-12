package cn.ttplatform.wh.core.data.tool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import cn.ttplatform.wh.data.tool.ByteBufferWriter;
import cn.ttplatform.wh.data.tool.DirectByteBufferPool;
import cn.ttplatform.wh.support.Pool;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Wang Hao
 * @date 2021/4/29 11:32
 */
@Slf4j
public class ByteBufferWriterTest {

    ByteBufferWriter byteBufferWriter;

    @Before
    public void setUp() throws IOException {
        Pool<ByteBuffer> bufferPool = new DirectByteBufferPool(10, 10 * 1024 * 1024);
        String property = System.getProperty("user.home");
        File file = File.createTempFile(property,"log.index");
        byteBufferWriter = new ByteBufferWriter(file, bufferPool);
    }

    @After
    public void tearDown() {
        byteBufferWriter.clear();
    }

    @Test
    public void writeIntAt() {
        long begin = System.nanoTime();
        byteBufferWriter.writeInt(0, 1);
        log.info("writeIntAt cost {} ns", (System.nanoTime() - begin));
    }

    @Test
    public void readIntAt() {
        byteBufferWriter.writeInt(0, 1);
        long begin = System.nanoTime();
        int res = byteBufferWriter.readInt(0);
        log.info("readIntAt cost {} ns", (System.nanoTime() - begin));
        log.info("readIntAt result is {}.", res);
    }

    @Test
    public void writeBytesAt() {
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, 1000000).forEach(sb::append);
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        long begin = System.nanoTime();
        byteBufferWriter.write(0, content);
        log.info("writeBytesAt cost {} ns", (System.nanoTime() - begin));
    }

    @Test
    public void append() {
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, 1000000).forEach(sb::append);
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        long begin = System.nanoTime();
        byteBufferWriter.append(content, content.length);
        log.info("append a chunk size[{}] cost {} ns", content.length, (System.nanoTime() - begin));
    }

    @Test
    public void readBytesAt() {
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, 1000000).forEach(sb::append);
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        byteBufferWriter.append(content, content.length);
        long begin = System.nanoTime();
        byteBufferWriter.readBytes(0, (int) byteBufferWriter.size());
        log.info("readBytesAt {} ns", (System.nanoTime() - begin));
    }

    @Test
    public void truncate() {
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, 1000000).forEach(sb::append);
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        byteBufferWriter.append(content, content.length);
        long begin = System.nanoTime();
        byteBufferWriter.truncate(1);
        log.info("truncate cost {} ns", (System.nanoTime() - begin));
    }

    @Test
    public void clear() {
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, 1000000).forEach(sb::append);
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        byteBufferWriter.append(content, content.length);
        long begin = System.nanoTime();
        byteBufferWriter.clear();
        log.info("clear cost {} ns", (System.nanoTime() - begin));
    }

    @Test
    public void isEmpty() {
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, 1000000).forEach(sb::append);
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        byteBufferWriter.append(content, content.length);
        assertFalse(byteBufferWriter.isEmpty());
        byteBufferWriter.clear();
        assertTrue(byteBufferWriter.isEmpty());
    }

    @Test
    public void size() {
        byteBufferWriter.clear();
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, 1000000).forEach(sb::append);
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        byteBufferWriter.append(content, content.length);
        assertEquals(content.length, byteBufferWriter.size());
        long offset = ThreadLocalRandom.current().nextLong(content.length);
        byteBufferWriter.truncate(offset);
        assertEquals(offset, byteBufferWriter.size());
        byteBufferWriter.clear();
        assertEquals(0L, byteBufferWriter.size());
    }

}