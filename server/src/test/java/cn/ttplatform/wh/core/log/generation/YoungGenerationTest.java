package cn.ttplatform.wh.core.log.generation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import cn.ttplatform.wh.core.log.entry.LogEntry;
import cn.ttplatform.wh.core.log.entry.LogEntryFactory;
import cn.ttplatform.wh.core.log.entry.LogEntryIndex;
import cn.ttplatform.wh.core.log.snapshot.FileSnapshot;
import cn.ttplatform.wh.core.log.tool.DirectByteBufferPool;
import cn.ttplatform.wh.support.ByteArrayPool;
import cn.ttplatform.wh.support.Pool;
import cn.ttplatform.wh.support.PooledByteBuffer;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Wang Hao
 * @date 2021/4/29 14:46
 */
@Slf4j
public class YoungGenerationTest {

    YoungGeneration youngGeneration;

    @Before
    public void setUp() {
        Pool<PooledByteBuffer> bufferPool = new DirectByteBufferPool(10, 10 * 1024 * 1024);
        Pool<byte[]> byteArrayPool = new ByteArrayPool(10, 10 * 1024 * 1024);
        String path = Objects.requireNonNull(YoungGenerationTest.class.getClassLoader().getResource("")).getPath();
        File file = new File(path);
        file.deleteOnExit();
        youngGeneration = new YoungGeneration(file, bufferPool, byteArrayPool, 0);
    }

    @After
    public void tearDown() {
        youngGeneration.fileLogEntry.removeAfter(0);
        youngGeneration.fileLogEntryIndex.removeAfter(0);
        youngGeneration.fileSnapshot.clear();
        youngGeneration.close();
    }

    @Test
    public void generateSnapshot() {
        int count = ThreadLocalRandom.current().nextInt(10000000);
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, count).forEach(sb::append);
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        long begin = System.nanoTime();
        youngGeneration.generateSnapshot(0, 0, content);
        log.debug("write {} byte cost {} ns", content.length + FileSnapshot.HEADER_LENGTH, (System.nanoTime() - begin));
    }

    @Test
    public void getLastLogMetaData() {
        LogEntryIndex lastLogMetaData = youngGeneration.getLastLogMetaData();
    }

    @Test
    public void getMaxLogIndex() {
    }

    @Test
    public void pendingEntry() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        IntStream.range(1, 10000).forEach(index -> {
            youngGeneration.pendingEntry(LogEntryFactory.createEntry(1, 1, index + 1, content, content.length));
        });
    }

    @Test
    public void pendingEntries() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        List<LogEntry> entries = new ArrayList<>();
        IntStream.range(1, 10000)
            .forEach(index -> entries.add(LogEntryFactory.createEntry(1, 1, index + 1, content, content.length)));
        youngGeneration.pendingEntries(entries);
    }

    @Test
    public void appendLogEntry() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        youngGeneration.appendLogEntry(LogEntryFactory.createEntry(1, 1, 1, content, content.length));
    }

    @Test
    public void appendLogEntries() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        List<LogEntry> entries = new ArrayList<>();
        IntStream.range(0, 10000).forEach(index -> {
            entries.add(LogEntryFactory.createEntry(1, 1, index + 1, content, content.length));
        });
        youngGeneration.appendLogEntries(entries);
    }

    @Test
    public void commit() {
        pendingEntries();
        youngGeneration.commit(1000);
        youngGeneration.commit(2000);
    }

    @Test
    public void getEntry() {
        appendLogEntries();
        IntStream.range(0, 10000).forEach(index -> {
            LogEntry entry = youngGeneration.getEntry(index + 1);
            assertEquals(index + 1, entry.getIndex());
        });
    }

    @Test
    public void getEntryMetaData() {
        appendLogEntries();
        LogEntryIndex entryMetaData = youngGeneration.getEntryMetaData(1000);
        assertEquals(1000, entryMetaData.getIndex());
    }

    @Test
    public void subList() {
        appendLogEntries();
        List<LogEntry> logEntries = youngGeneration.subList(1, 1000);
        assertEquals(998, logEntries.size());
    }

    @Test
    public void removeAfter() {
        appendLogEntries();
        youngGeneration.removeAfter(1000);
        LogEntryIndex entryMetaData = youngGeneration.getEntryMetaData(1000);
        assertEquals(1000, entryMetaData.getIndex());
        entryMetaData = youngGeneration.getEntryMetaData(1001);
        assertNull(entryMetaData);
    }

    @Test
    public void writeSnapshot() {
        int count = ThreadLocalRandom.current().nextInt(10000000);
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, count).forEach(sb::append);
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        long begin = System.nanoTime();
        youngGeneration.writeSnapshot(content);
        log.debug("append {} bytes cost {} ns", content.length, (System.nanoTime() - begin));
    }

    @Test
    public void clearSnapshot() {
        generateSnapshot();
        assertFalse(youngGeneration.fileSnapshot.isEmpty());
        youngGeneration.clearSnapshot();
        assertTrue(youngGeneration.fileSnapshot.isEmpty());
    }

    @Test
    public void isEmpty() {
        assertTrue(youngGeneration.isEmpty());
        appendLogEntries();
        assertFalse(youngGeneration.isEmpty());
    }
}