package cn.ttplatform.wh.core.log.entry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import cn.ttplatform.wh.core.log.tool.DirectByteBufferPool;
import cn.ttplatform.wh.support.ByteArrayPool;
import cn.ttplatform.wh.support.Pool;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Wang Hao
 * @date 2021/4/29 11:56
 */
@Slf4j
class FileLogEntryIndexTest {

    FileLogEntryIndex fileLogEntryIndex;

    @Before
    void setUp() {
        Pool<ByteBuffer> bufferPool = new DirectByteBufferPool(10, 10 * 1024 * 1024);
        Pool<byte[]> byteArrAyPool = new ByteArrayPool(10, 10 * 1024 * 1024);
        String path = Objects.requireNonNull(FileLogEntryIndexTest.class.getClassLoader().getResource("")).getPath();
        fileLogEntryIndex = new FileLogEntryIndex(new File(path), bufferPool, byteArrAyPool, 0);
    }

    @After
    void tearDown() {
        fileLogEntryIndex.removeAfter(0);
        fileLogEntryIndex.close();
    }

    @Test
    void getLastEntryIndex() {
        fileLogEntryIndex.append(LogEntryFactory.createEntry(1, 1, 1, new byte[0]), 0L);
        LogEntryIndex lastEntryIndex = fileLogEntryIndex.getLastEntryIndex();
        assertNotNull(lastEntryIndex);
    }

    @Test
    void getEntryOffset() {
        fileLogEntryIndex.append(LogEntryFactory.createEntry(1, 1, 1, new byte[0]), 0L);
        long entryOffset = fileLogEntryIndex.getEntryOffset(1);
        assertEquals(0, entryOffset);
        entryOffset = fileLogEntryIndex.getEntryOffset(2);
        assertEquals(-1, entryOffset);
    }

    @Test
    void getEntryMetaData() {
        fileLogEntryIndex.append(LogEntryFactory.createEntry(1, 1, 1, new byte[0]), 0L);
        LogEntryIndex entryMetaData = fileLogEntryIndex.getEntryMetaData(1);
        assertEquals(0, entryMetaData.getOffset());
        entryMetaData = fileLogEntryIndex.getEntryMetaData(2);
        assertNull(entryMetaData);
    }

    @Test
    void testAppend() {
        int capacity = ThreadLocalRandom.current().nextInt(1000000);
        List<LogEntry> logEntries = new ArrayList<>(capacity);
        long[] offsets = new long[capacity];
        IntStream.range(0, capacity).forEach(index -> {
            logEntries.add(LogEntryFactory.createEntry(1, 1, index + 1, new byte[0]));
            offsets[index] = index;
        });
        long begin = System.nanoTime();
        fileLogEntryIndex.append(logEntries, offsets);
        log.debug("append {} entry indices cost {} ns", capacity, (System.nanoTime() - begin));
    }

}