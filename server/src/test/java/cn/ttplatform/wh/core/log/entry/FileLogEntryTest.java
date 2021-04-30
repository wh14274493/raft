package cn.ttplatform.wh.core.log.entry;

import static org.junit.jupiter.api.Assertions.*;

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
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Wang Hao
 * @date 2021/4/29 12:31
 */
@Slf4j
class FileLogEntryTest {

    FileLogEntry fileLogEntry;

    @BeforeEach
    void setUp() {
        BufferPool<ByteBuffer> bufferPool = new DirectByteBufferPool(10, 10 * 1024 * 1024);
        String path = Objects.requireNonNull(FileLogEntryIndexTest.class.getClassLoader().getResource("")).getPath();
        fileLogEntry = new FileLogEntry(new File(path), bufferPool);
    }

    @AfterEach
    void tearDown() {
        fileLogEntry.removeAfter(0L);
        assertEquals(0, fileLogEntry.size());
    }

    @Test
    void append() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        long begin = System.nanoTime();
        fileLogEntry.append(LogEntryFactory.createEntry(1, 1, 1, content));
        log.debug("append 1 entry indices cost {} ns", (System.nanoTime() - begin));
    }

    @Test
    void testAppend() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        int capacity = ThreadLocalRandom.current().nextInt(1000000);
        List<LogEntry> logEntries = new ArrayList<>(capacity);
        IntStream.range(0, capacity).forEach(index -> logEntries.add(LogEntryFactory.createEntry(1, 1, index + 1, content)));
        long begin = System.nanoTime();
        long[] append = fileLogEntry.append(logEntries);
        log.debug("append {} entry indices cost {} ns", capacity, (System.nanoTime() - begin));
        assertEquals(capacity, append.length);
    }

    @Test
    void getEntry() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        fileLogEntry.append(LogEntryFactory.createEntry(1, 1, 1, content));
        long begin = System.nanoTime();
        LogEntry entry = fileLogEntry.getEntry(0, fileLogEntry.size());
        log.debug("load 1 entry index cost {} ns", (System.nanoTime() - begin));
        assertEquals(1, entry.getIndex());
    }

    @Test
    void loadEntriesFromFile() {
        testAppend();
        long begin = System.nanoTime();
        byte[] bytes = fileLogEntry.loadEntriesFromFile(0, fileLogEntry.size());
        log.debug("load {} bytes cost {} ns", bytes.length, (System.nanoTime() - begin));
        assertEquals(fileLogEntry.size(), bytes.length);
    }

}