package cn.ttplatform.wh.data;

import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.data.log.Log;
import cn.ttplatform.wh.data.log.LogFactory;
import cn.ttplatform.wh.data.tool.DirectByteBufferPool;
import cn.ttplatform.wh.group.Cluster;
import cn.ttplatform.wh.group.Endpoint;
import cn.ttplatform.wh.support.Message;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Wang Hao
 * @date 2021/6/5 9:50
 */
public class DataManagerTest {

    DataManager dataManager;

    @Before
    public void setUp() throws Exception {
        ServerProperties properties = new ServerProperties();
        GlobalContext context = GlobalContext.builder().properties(properties)
            .byteBufferPool(new DirectByteBufferPool(10, 16 * 1024 * 1024)).build();
        Cluster cluster = new Cluster(context);
        context.setCluster(cluster);
        dataManager = new DataManager(context);
    }

    @Test
    public void getLastIncludeIndex() {
        Assert.assertEquals(0, dataManager.getLastIncludeIndex());
    }

    @Test
    public void getIndexOfLastLog() {
        Assert.assertEquals(0, dataManager.getIndexOfLastLog());
    }

    @Test
    public void getTermOfLastLog() {
        Assert.assertEquals(0, dataManager.getTermOfLastLog());
    }

    @Test
    public void getTermOfLog() {
        dataManager.pendingLog(LogFactory.createEntry(0, 1, 1, new byte[0], 0));
        Assert.assertEquals(1, dataManager.getTermOfLog(1));
    }

    @Test
    public void isNewerThan() {
        dataManager.pendingLog(LogFactory.createEntry(0, 1, 1, new byte[0], 0));
        dataManager.pendingLog(LogFactory.createEntry(0, 1, 2, new byte[0], 0));
        Assert.assertTrue(dataManager.isNewerThan(1, 1));
    }

    public void pendingLogs() {
        int count = 10000;
        List<Log> logs = new ArrayList<>(count);
        IntStream.range(0, count).forEach(index -> {
            logs.add(LogFactory.createEntry(0, 1, index, new byte[0], 0));
        });
        dataManager.pendingLogs(0, logs);
    }

    @Test
    public void checkIndexAndTermIfMatched() {
        pendingLogs();
        Assert.assertTrue(dataManager.checkIndexAndTermIfMatched(1, 1));
        Assert.assertFalse(dataManager.checkIndexAndTermIfMatched(10001, 1));
    }

    @Test
    public void createAppendLogEntriesMessage() {
        pendingLogs();
        Endpoint endpoint = new Endpoint("b,localhost,1111");
        Message message = dataManager.createAppendLogEntriesMessage("a", 1, endpoint, 100);
        Assert.assertNull(message);
        endpoint.setNextIndex(1);
        message = dataManager.createAppendLogEntriesMessage("a", 1, endpoint, 100);
        Assert.assertNotNull(message);
    }

//    @Test
//    public void advanceCommitIndex() {
//        pendingLogs();
//        Assert.assertTrue(logManager.advanceCommitIndex(100, 1));
//    }

//    @Test
//    public void installSnapshot() {
//        int count = 1000000;
//        byte[] bytes = new byte[count];
//        IntStream.range(0, count).forEach(index -> bytes[index] = 1);
//        InstallSnapshotMessage message = InstallSnapshotMessage.builder().term(1).chunk(bytes).lastIncludeIndex(1).lastIncludeTerm(1)
//            .done(false).sourceId("a").offset(0L).build();
//        Assert.assertTrue(logManager.installSnapshot(message));
//    }

    @Test
    public void getLog() {
        pendingLogs();
        Assert.assertEquals(1, dataManager.getLog(1).getIndex());
    }

    @Test
    public void range() {
        pendingLogs();
        dataManager.range(1,5000);
    }

}