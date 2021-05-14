package cn.ttplatform.wh.core.log;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.core.GlobalContext;
import cn.ttplatform.wh.core.group.Endpoint;
import cn.ttplatform.wh.core.group.EndpointMetaData;
import cn.ttplatform.wh.core.log.entry.FileLogEntryIndexTest;
import cn.ttplatform.wh.core.log.entry.LogEntry;
import cn.ttplatform.wh.core.log.entry.LogEntryFactory;
import cn.ttplatform.wh.core.log.tool.DirectByteBufferPool;
import cn.ttplatform.wh.support.ByteArrayPool;
import cn.ttplatform.wh.support.FixedSizeLinkedBufferPool;
import cn.ttplatform.wh.support.Message;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Wang Hao
 * @date 2021/5/12 22:07
 */
@Slf4j
public class FileLogTest {

    FileLog fileLog;

    @Before
    public void setUp() throws Exception {
        String path = Objects.requireNonNull(FileLogEntryIndexTest.class.getClassLoader().getResource("")).getPath();
        GlobalContext context = GlobalContext.builder()
            .properties(new ServerProperties(path))
            .byteArrayPool(new ByteArrayPool(10, 10 * 1024 * 1024))
            .byteBufferPool(new DirectByteBufferPool(10, 10 * 1024 * 1024))
            .linkedBufferPool(new FixedSizeLinkedBufferPool(10))
            .build();
        fileLog = new FileLog(context);
    }

    @Test
    public void getLastIncludeIndex() {
        Assert.assertEquals(0, fileLog.getLastIncludeIndex());
    }

    @Test
    public void getLastLogIndex() {
        Assert.assertEquals(0, fileLog.getLastLogIndex());
    }

    @Test
    public void getLastLogTerm() {
        Assert.assertEquals(0, fileLog.getLastLogTerm());
    }

    @Test
    public void isNewerThan() {
        boolean newerThan = fileLog.isNewerThan(1, 1);
        Assert.assertFalse(newerThan);
    }

    @Test
    public void getNextIndex() {
        Assert.assertEquals(1, fileLog.getNextIndex());
    }

    @Test
    public void getEntry() {
        LogEntry entry = fileLog.getEntry(0);
        Assert.assertNull(entry);
    }

    @Test
    public void subList() {
        List<LogEntry> logEntries = fileLog.subList(0, 1);
        Assert.assertEquals(0, logEntries.size());
    }

    @Test
    public void pendingEntries() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        int capacity = ThreadLocalRandom.current().nextInt(900000, 1000000);
        List<LogEntry> logEntries = new ArrayList<>(capacity);
        IntStream.range(0, capacity)
            .forEach(index -> logEntries.add(LogEntryFactory.createEntry(1, 1, index + 1, content, content.length)));
        long begin = System.nanoTime();
        fileLog.pendingEntries(0, logEntries);
        log.info("pending {} logs cost {} ns.", capacity, System.nanoTime() - begin);
        Assert.assertEquals(capacity, fileLog.getLastLogIndex());
    }

    @Test
    public void checkIndexAndTermIfMatched() {
        pendingEntries();
        boolean m = fileLog.checkIndexAndTermIfMatched(1, 1);
        Assert.assertTrue(m);
    }

    @Test
    public void pendingEntry() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        long begin = System.nanoTime();
        fileLog.pendingEntry(LogEntryFactory.createEntry(1, 1, 1, content, content.length));
        log.info("pending 1 logs cost {} ns.", System.nanoTime() - begin);
        Assert.assertEquals(1, fileLog.getLastLogIndex());
    }

    @Test
    public void createAppendLogEntriesMessage() {
        Endpoint endpoint = new Endpoint(new EndpointMetaData("B", "127.0.0.1", 1111));
        pendingEntries();
        Message message = fileLog.createAppendLogEntriesMessage("A", 1, endpoint, 100);
        Assert.assertNull(message);
    }

    @Test
    public void createInstallSnapshotMessage() {
        Message installSnapshotMessage = fileLog.createInstallSnapshotMessage(0, 0, 100);
        Assert.assertNotNull(installSnapshotMessage);

    }

    @Test
    public void advanceCommitIndex() {
        pendingEntries();
        long begin = System.nanoTime();
        boolean res = fileLog.advanceCommitIndex(900000, 1);
        log.info("pending 900000 logs cost {} ns.", System.nanoTime() - begin);
    }

    @Test
    public void shouldGenerateSnapshot() {
        boolean shouldGenerateSnapshot = fileLog.shouldGenerateSnapshot(1024);
        Assert.assertFalse(shouldGenerateSnapshot);
    }

    @Test
    public void installSnapshot() {
    }

    @Test
    public void generateSnapshot() {
    }

    @Test
    public void getSnapshotData() {
    }

    @Test
    public void close() {
    }
}