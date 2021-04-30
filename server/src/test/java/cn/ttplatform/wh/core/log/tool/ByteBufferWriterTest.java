package cn.ttplatform.wh.core.log.tool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import cn.ttplatform.wh.support.BufferPool;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
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
class ByteBufferWriterTest {

    ByteBufferWriter byteBufferWriter;

    @Before
    void setUp() {
        BufferPool<ByteBuffer> bufferPool = new DirectByteBufferPool(10, 10 * 1024 * 1024);
        String path = Objects.requireNonNull(ByteBufferWriterTest.class.getClassLoader().getResource("test.txt")).getPath();
        byteBufferWriter = new ByteBufferWriter(new File(path), bufferPool);
    }

    @After
    void tearDown() {
        byteBufferWriter.clear();
    }

    @Test
    void writeIntAt() {
        long begin = System.nanoTime();
        byteBufferWriter.writeIntAt(0, 1);
        log.debug("writeIntAt cost {} ns", (System.nanoTime() - begin));
    }

    @Test
    void readIntAt() {
        byteBufferWriter.writeIntAt(0, 1);
        long begin = System.nanoTime();
        int res = byteBufferWriter.readIntAt(0);
        log.debug("readIntAt cost {} ns", (System.nanoTime() - begin));
        log.debug("readIntAt result is {}.", res);
    }

    @Test
    void writeBytesAt() {
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, 1000000).forEach(sb::append);
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        long begin = System.nanoTime();
        byteBufferWriter.writeBytesAt(0, content);
        log.debug("writeBytesAt cost {} ns", (System.nanoTime() - begin));
    }

    @Test
    void append() {
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, 1000000).forEach(sb::append);
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        long begin = System.nanoTime();
        byteBufferWriter.append(content);
        log.debug("append a chunk size[{}] cost {} ns", content.length, (System.nanoTime() - begin));
    }

    @Test
    void readBytesAt() {
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, 1000000).forEach(sb::append);
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        byteBufferWriter.append(content);
        long begin = System.nanoTime();
        byteBufferWriter.readBytesAt(0, (int) byteBufferWriter.size());
        log.debug("readBytesAt {} ns", (System.nanoTime() - begin));
    }

    @Test
    void truncate() {
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, 1000000).forEach(sb::append);
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        byteBufferWriter.append(content);
        long begin = System.nanoTime();
        byteBufferWriter.truncate(1);
        log.debug("truncate cost {} ns", (System.nanoTime() - begin));
    }

    @Test
    void clear() {
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, 1000000).forEach(sb::append);
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        byteBufferWriter.append(content);
        long begin = System.nanoTime();
        byteBufferWriter.clear();
        log.debug("clear cost {} ns", (System.nanoTime() - begin));
    }

    @Test
    void isEmpty() {
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, 1000000).forEach(sb::append);
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        byteBufferWriter.append(content);
        assertFalse(byteBufferWriter.isEmpty());
        byteBufferWriter.clear();
        assertTrue(byteBufferWriter.isEmpty());
    }

    @Test
    void size() {
        byteBufferWriter.clear();
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, 1000000).forEach(sb::append);
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        byteBufferWriter.append(content);
        assertEquals(content.length, byteBufferWriter.size());
        long offset = ThreadLocalRandom.current().nextLong(content.length);
        byteBufferWriter.truncate(offset);
        assertEquals(offset, byteBufferWriter.size());
        byteBufferWriter.clear();
        assertEquals(0L, byteBufferWriter.size());
    }

}