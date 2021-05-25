//package cn.ttplatform.wh.core.data.snapshot;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertFalse;
//import static org.junit.Assert.assertNull;
//import static org.junit.Assert.assertTrue;
//import static org.junit.Assert.assertEquals;
//
//import cn.ttplatform.wh.core.data.log.LogFileIndexTest;
//import cn.ttplatform.wh.core.data.tool.DirectByteBufferPool;
//import cn.ttplatform.wh.support.ByteArrayPool;
//import cn.ttplatform.wh.support.Pool;
//import cn.ttplatform.wh.core.data.tool.PooledByteBuffer;
//import java.io.File;
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//import java.nio.file.Files;
//import java.util.Objects;
//import java.util.concurrent.ThreadLocalRandom;
//import java.util.stream.IntStream;
//import lombok.extern.slf4j.Slf4j;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//
///**
// * @author Wang Hao
// * @date 2021/4/29 12:46
// */
//@Slf4j
//public class SnapshotFileTest {
//
//    SnapshotFile snapshotFile;
//    byte[] content;
//
//    @Before
//    public void setUp() throws IOException {
//        Pool<PooledByteBuffer> bufferPool = new DirectByteBufferPool(10, 10 * 1024 * 1024);
//        String path = Objects.requireNonNull(SnapshotFileTest.class.getClassLoader().getResource("")).getPath();
//        File file = new File(path, "log.data");
//        Files.deleteIfExists(file.toPath());
//        snapshotFile = new SnapshotFile(file, bufferPool);
//        int count = ThreadLocalRandom.current().nextInt(10000000);
//        StringBuilder sb = new StringBuilder();
//        IntStream.range(0, count).forEach(sb::append);
//        content = sb.toString().getBytes(StandardCharsets.UTF_8);
//        long begin = System.nanoTime();
//    }
//
//    @After
//    public void tearDown() {
//        snapshotFile.clear();
//        assertTrue(snapshotFile.isEmpty());
//        snapshotFile.close();
//    }
//
//    @Test
//    public void read() {
//        long begin = System.nanoTime();
//        int read = snapshotFile.read(0, content.length + SnapshotFile.HEADER_LENGTH).length;
//        log.info("read {} bytes cost {} ns", content.length + SnapshotFile.HEADER_LENGTH, (System.nanoTime() - begin));
//        assertEquals(content.length + SnapshotFile.HEADER_LENGTH, read);
//    }
//
//    @Test
//    public void readAll() {
//        long begin = System.nanoTime();
//        int read = snapshotFile.readAll().limit();
//        log.info("read {} bytes cost {} ns", content.length, (System.nanoTime() - begin));
//        assertEquals(content.length, read);
//    }
//
//    @Test
//    public void getSnapshotHeader() {
//        SnapshotHeader snapshotHeader = snapshotFile.getSnapshotHeader();
//        assertEquals(0, snapshotHeader.getLastIncludeIndex());
//        assertEquals(0, snapshotHeader.getLastIncludeTerm());
//    }
//}