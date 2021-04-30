package cn.ttplatform.wh.core.log.generation;

import cn.ttplatform.wh.core.log.entry.LogEntry;
import cn.ttplatform.wh.core.log.entry.LogEntryFactory;
import cn.ttplatform.wh.core.log.entry.LogEntryIndex;
import cn.ttplatform.wh.core.log.snapshot.FileSnapshot;
import cn.ttplatform.wh.core.log.tool.DirectByteBufferPool;
import cn.ttplatform.wh.support.BufferPool;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Wang Hao
 * @date 2021/4/29 14:46
 */
@Slf4j
class YoungGenerationTest {

    YoungGeneration youngGeneration;

    @BeforeEach
    void setUp() {
        BufferPool<ByteBuffer> bufferPool = new DirectByteBufferPool(10, 10 * 1024 * 1024);
        String path = Objects.requireNonNull(YoungGenerationTest.class.getClassLoader().getResource("")).getPath();
        File file = new File(path);
        file.deleteOnExit();
        youngGeneration = new YoungGeneration(file, bufferPool, 0);
    }

    @AfterEach
    void tearDown() throws NoSuchFieldException {
        youngGeneration.fileLogEntry.removeAfter(0);
        youngGeneration.fileLogEntryIndex.removeAfter(0);
        youngGeneration.fileSnapshot.clear();
        youngGeneration.close();
    }

    @Test
    void generateSnapshot() {
        int count = ThreadLocalRandom.current().nextInt(10000000);
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, count).forEach(sb::append);
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        long begin = System.nanoTime();
        youngGeneration.generateSnapshot(0, 0, content);
        log.debug("write {} byte cost {} ns", content.length + FileSnapshot.HEADER_LENGTH, (System.nanoTime() - begin));
    }

    @Test
    void getLastLogMetaData() {
        LogEntryIndex lastLogMetaData = youngGeneration.getLastLogMetaData();
    }

    @Test
    void getMaxLogIndex() {
    }

    @Test
    void pendingEntry() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        IntStream.range(1, 10000).forEach(index -> {
            youngGeneration.pendingEntry(LogEntryFactory.createEntry(1, 1, index + 1, content));
        });
    }

    @Test
    void pendingEntries() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        List<LogEntry> entries = new ArrayList<>();
        IntStream.range(1, 10000).forEach(index -> entries.add(LogEntryFactory.createEntry(1, 1, index + 1, content)));
        youngGeneration.pendingEntries(entries);
    }

    @Test
    void appendLogEntry() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        youngGeneration.appendLogEntry(LogEntryFactory.createEntry(1, 1, 1, content));
    }

    @Test
    void appendLogEntries() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        List<LogEntry> entries = new ArrayList<>();
        IntStream.range(1, 10000).forEach(index -> {
            entries.add(LogEntryFactory.createEntry(1, 1, index + 1, content));
        });
        youngGeneration.appendLogEntries(entries);
    }

    @Test
    void commit() {
        pendingEntries();
        youngGeneration.commit(1000);
        youngGeneration.commit(2000);
    }

    @Test
    void getEntry() {
        appendLogEntries();
        LogEntry entry = youngGeneration.getEntry(1000);
        Assertions.assertEquals(1000, entry.getIndex());
    }

    @Test
    void getEntryMetaData() {
        appendLogEntries();
        LogEntryIndex entryMetaData = youngGeneration.getEntryMetaData(1000);
        Assertions.assertEquals(1000, entryMetaData.getIndex());
    }

    @Test
    void subList() {
        appendLogEntries();
        List<LogEntry> logEntries = youngGeneration.subList(1, 1000);
        Assertions.assertEquals(998, logEntries.size());
    }

    @Test
    void removeAfter() {
        appendLogEntries();
        youngGeneration.removeAfter(1000);
        LogEntryIndex entryMetaData = youngGeneration.getEntryMetaData(1000);
        Assertions.assertEquals(1000, entryMetaData.getIndex());
        entryMetaData = youngGeneration.getEntryMetaData(1001);
        Assertions.assertNull(entryMetaData);
    }

    @Test
    void writeSnapshot() {
        int count = ThreadLocalRandom.current().nextInt(10000000);
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, count).forEach(sb::append);
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        long begin = System.nanoTime();
        youngGeneration.writeSnapshot(content);
        log.debug("append {} bytes cost {} ns", content.length, (System.nanoTime() - begin));
    }

    @Test
    void clearSnapshot() {
        generateSnapshot();
        Assertions.assertFalse(youngGeneration.fileSnapshot.isEmpty());
        youngGeneration.clearSnapshot();
        Assertions.assertTrue(youngGeneration.fileSnapshot.isEmpty());
    }

    @Test
    void isEmpty() {
        Assertions.assertTrue(youngGeneration.isEmpty());
        appendLogEntries();
        Assertions.assertFalse(youngGeneration.isEmpty());
    }
}