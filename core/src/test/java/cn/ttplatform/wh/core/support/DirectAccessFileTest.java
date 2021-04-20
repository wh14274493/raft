package cn.ttplatform.wh.core.support;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Wang Hao
 * @date 2021/4/19 13:21
 */
class DirectAccessFileTest {

    DirectAccessFile directAccessFile;
    byte[] content;

    @BeforeEach
    void setUp() {
        File file = new File("D:\\workspace\\java\\raft\\core\\src\\test\\resources\\log.data");
        directAccessFile = new DirectAccessFile(file, new DirectByteBufferPool(10, 10 * 1024 * 1014));
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, 1000000).forEach(index -> {
            sb.append(index);
        });
        content = sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void writeBytes() {
        System.out.println("contentLength = " + content.length);
        System.out.println("before fileSize = " + directAccessFile.size());
        long begin = System.currentTimeMillis();
        directAccessFile.writeBytes(content);
        System.out.println(System.currentTimeMillis() - begin);
        System.out.println("after fileSize = " + directAccessFile.size());
    }

    @Test
    void writeBytesAt() {
        System.out.println("contentLength = " + content.length);
        System.out.println("before fileSize = " + directAccessFile.size());
        long begin = System.currentTimeMillis();
        directAccessFile.writeBytesAt(100L, content);
        System.out.println(System.currentTimeMillis() - begin);
        System.out.println("after fileSize = " + directAccessFile.size());
    }

    @Test
    void writeInt() {
        System.out.println("before fileSize = " + directAccessFile.size());
        long begin = System.currentTimeMillis();
        directAccessFile.writeInt(Integer.MAX_VALUE);
        System.out.println(System.currentTimeMillis() - begin);
        System.out.println("after fileSize = " + directAccessFile.size());
    }

    @Test
    void writeIntAt() {
        System.out.println("before fileSize = " + directAccessFile.size());
        long begin = System.nanoTime();
        directAccessFile.writeIntAt(100L, 100);
        System.out.println(System.nanoTime() - begin);
        System.out.println("after fileSize = " + directAccessFile.size());
    }

    @Test
    void writeLong() {
        System.out.println("before fileSize = " + directAccessFile.size());
        long begin = System.nanoTime();
        directAccessFile.writeLong(100L);
        System.out.println(System.nanoTime() - begin);
        System.out.println("after fileSize = " + directAccessFile.size());
    }

    @Test
    void writeLongAt() {
        System.out.println("before fileSize = " + directAccessFile.size());
        long begin = System.nanoTime();
        directAccessFile.writeLongAt(0L, Long.MAX_VALUE);
        System.out.println(System.nanoTime() - begin);
        System.out.println("after fileSize = " + directAccessFile.size());
    }

    @Test
    void readBytes() {

    }

    @Test
    void readBytesAt() {
        long begin = System.currentTimeMillis();
        byte[] bytes = directAccessFile.readBytesAt(0L, (int) directAccessFile.size());
        System.out.println(System.currentTimeMillis() - begin);
    }

    @Test
    void readInt() {
        long begin = System.nanoTime();
        directAccessFile.readInt();
        System.out.println(System.nanoTime() - begin);
    }

    @Test
    void readIntAt() {
        long begin = System.nanoTime();
        directAccessFile.readIntAt(0L);
        System.out.println(System.nanoTime() - begin);
    }

    @Test
    void readLong() {
        long begin = System.nanoTime();
        directAccessFile.readLong();
        System.out.println(System.nanoTime() - begin);
    }

    @Test
    void readLongAt() {
        long begin = System.nanoTime();
        directAccessFile.readLongAt(1L);
        System.out.println(System.nanoTime() - begin);
    }

    @Test
    void clear() {
        directAccessFile.clear();
    }
}