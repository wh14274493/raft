package cn.ttplatform.wh.core.data;

import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.data.DataManager;
import cn.ttplatform.wh.data.log.Log;
import cn.ttplatform.wh.data.log.LogFactory;
import cn.ttplatform.wh.data.tool.DirectByteBufferPool;
import cn.ttplatform.wh.group.Endpoint;
import cn.ttplatform.wh.group.EndpointMetaData;
import cn.ttplatform.wh.support.FixedSizeLinkedBufferPool;
import cn.ttplatform.wh.support.Message;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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
public class LogRegistryTest {

    DataManager fileLog;

    @Before
    public void setUp() throws Exception {
        String property = System.getProperty("user.home");
        GlobalContext context = GlobalContext.builder()
            .properties(new ServerProperties())
            .byteBufferPool(new DirectByteBufferPool(10, 10 * 1024 * 1024))
            .linkedBufferPool(new FixedSizeLinkedBufferPool(10))
            .build();
        fileLog = new DataManager(context);
    }

    @Test
    public void getLastIncludeIndex() {
        Assert.assertEquals(0, fileLog.getLastIncludeIndex());
    }

    @Test
    public void getLastLogIndex() {
        Assert.assertEquals(0, fileLog.getIndexOfLastLog());
    }

    @Test
    public void getLastLogTerm() {
        Assert.assertEquals(0, fileLog.getTermOfLastLog());
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
        Log entry = fileLog.getLog(0);
        Assert.assertNull(entry);
    }

    @Test
    public void subList() {
        List<Log> logEntries = fileLog.range(0, 1);
        Assert.assertEquals(0, logEntries.size());
    }

    @Test
    public void pendingEntries() {
        byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        int capacity = ThreadLocalRandom.current().nextInt(900000, 1000000);
        List<Log> logEntries = new ArrayList<>(capacity);
        IntStream.range(0, capacity)
            .forEach(index -> logEntries.add(LogFactory.createEntry(1, 1, index + 1, content, content.length)));
        long begin = System.nanoTime();
        fileLog.pendingLogs(0, logEntries);
        log.info("pending {} logs cost {} ns.", capacity, System.nanoTime() - begin);
        Assert.assertEquals(capacity, fileLog.getIndexOfLastLog());
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
        fileLog.pendingLog(LogFactory.createEntry(1, 1, 1, content, content.length));
        log.info("pending 1 logs cost {} ns.", System.nanoTime() - begin);
        Assert.assertEquals(1, fileLog.getIndexOfLastLog());
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