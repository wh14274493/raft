package cn.ttplatform.wh.core.log.snapshot;

import cn.ttplatform.wh.core.log.tool.DirectByteBufferPool;
import cn.ttplatform.wh.support.BufferPool;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Wang Hao
 * @date 2021/4/29 12:46
 */
@Slf4j
class FileSnapshotTest {

    FileSnapshot fileSnapshot;
    byte[] content;

    @BeforeEach
    void setUp() {
        BufferPool<ByteBuffer> bufferPool = new DirectByteBufferPool(10, 10 * 1024 * 1024);
        String path = Objects.requireNonNull(FileSnapshotTest.class.getClassLoader().getResource("")).getPath();
        fileSnapshot = new FileSnapshot(new File(path), bufferPool, true);
        int count = ThreadLocalRandom.current().nextInt(10000000);
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, count).forEach(sb::append);
        content = sb.toString().getBytes(StandardCharsets.UTF_8);
        long begin = System.nanoTime();
        fileSnapshot.write(0, 0, content);
        log.debug("write {} byte cost {} ns", content.length + FileSnapshot.HEADER_LENGTH, (System.nanoTime() - begin));
    }

    @AfterEach
    void tearDown() {
        fileSnapshot.clear();
        Assertions.assertEquals(true, fileSnapshot.isEmpty());
        fileSnapshot.close();
    }

    @Test
    void read() {
        long begin = System.nanoTime();
        int read = fileSnapshot.read(0, content.length + FileSnapshot.HEADER_LENGTH).length;
        log.debug("read {} bytes cost {} ns", content.length + FileSnapshot.HEADER_LENGTH, (System.nanoTime() - begin));
        Assertions.assertEquals(content.length + FileSnapshot.HEADER_LENGTH, read);
    }

    @Test
    void readAll() {
        long begin = System.nanoTime();
        int read = fileSnapshot.readAll().length;
        log.debug("read {} bytes cost {} ns", content.length, (System.nanoTime() - begin));
        Assertions.assertEquals(content.length, read);
    }

    @Test
    void append() {
        long begin = System.nanoTime();
        fileSnapshot.append(content);
        log.debug("append {} bytes cost {} ns", content.length, (System.nanoTime() - begin));
        int read = fileSnapshot.read(FileSnapshot.HEADER_LENGTH, content.length * 2).length;
        log.debug("read {} bytes cost {} ns", read, (System.nanoTime() - begin));
        Assertions.assertEquals(content.length * 2, read);
    }

    @Test
    void isEmpty() {
        Assertions.assertFalse(fileSnapshot.isEmpty());
    }

    @Test
    void getSnapshotHeader() {
        SnapshotHeader snapshotHeader = fileSnapshot.getSnapshotHeader();
        Assertions.assertEquals(0, snapshotHeader.getLastIncludeIndex());
        Assertions.assertEquals(0, snapshotHeader.getLastIncludeTerm());
        Assertions.assertEquals(content.length, snapshotHeader.getContentLength());
    }
}