package support;

import cn.ttplatform.wh.core.support.ByteBufferWriter;
import cn.ttplatform.wh.core.support.DirectByteBufferPool;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.stream.IntStream;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Wang Hao
 * @date 2021/4/19 13:21
 */
class ByteBufferWriterTest {

    ByteBufferWriter byteBufferWriter;
    byte[] content;

    @BeforeEach
    void setUp() {
        File file = new File("D:\\workspace\\java\\raft\\core\\src\\test\\resources\\log.data");
        byteBufferWriter = new ByteBufferWriter(file, new DirectByteBufferPool(10, 10 * 1024 * 1014));
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, 1000000).forEach(index -> {
            sb.append(index);
        });
        content = sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void writeBytes() {
        System.out.println("contentLength = " + content.length);
        System.out.println("before fileSize = " + byteBufferWriter.size());
        long begin = System.currentTimeMillis();
        byteBufferWriter.writeBytes(content);
        System.out.println(System.currentTimeMillis() - begin);
        System.out.println("after fileSize = " + byteBufferWriter.size());
    }

    @Test
    void writeBytesAt() {
        System.out.println("contentLength = " + content.length);
        System.out.println("before fileSize = " + byteBufferWriter.size());
        long begin = System.currentTimeMillis();
        byteBufferWriter.writeBytesAt(100L, content);
        System.out.println(System.currentTimeMillis() - begin);
        System.out.println("after fileSize = " + byteBufferWriter.size());
    }

    @Test
    void writeInt() {
        System.out.println("before fileSize = " + byteBufferWriter.size());
        long begin = System.currentTimeMillis();
        byteBufferWriter.writeInt(Integer.MAX_VALUE);
        System.out.println(System.currentTimeMillis() - begin);
        System.out.println("after fileSize = " + byteBufferWriter.size());
    }

    @Test
    void writeIntAt() {
        System.out.println("before fileSize = " + byteBufferWriter.size());
        long begin = System.nanoTime();
        byteBufferWriter.writeIntAt(100L, 100);
        System.out.println(System.nanoTime() - begin);
        System.out.println("after fileSize = " + byteBufferWriter.size());
    }

    @Test
    void writeLong() {
        System.out.println("before fileSize = " + byteBufferWriter.size());
        long begin = System.nanoTime();
        byteBufferWriter.writeLong(100L);
        System.out.println(System.nanoTime() - begin);
        System.out.println("after fileSize = " + byteBufferWriter.size());
    }

    @Test
    void writeLongAt() {
        System.out.println("before fileSize = " + byteBufferWriter.size());
        long begin = System.nanoTime();
        byteBufferWriter.writeLongAt(0L, Long.MAX_VALUE);
        System.out.println(System.nanoTime() - begin);
        System.out.println("after fileSize = " + byteBufferWriter.size());
    }

    @Test
    void readBytes() {

    }

    @Test
    void readBytesAt() {
        long begin = System.currentTimeMillis();
        byte[] bytes = byteBufferWriter.readBytesAt(0L, (int) byteBufferWriter.size());
        System.out.println(System.currentTimeMillis() - begin);
    }

    @Test
    void readInt() {
        long begin = System.nanoTime();
        byteBufferWriter.readInt();
        System.out.println(System.nanoTime() - begin);
    }

    @Test
    void readIntAt() {
        long begin = System.nanoTime();
        byteBufferWriter.readIntAt(0L);
        System.out.println(System.nanoTime() - begin);
    }

    @Test
    void readLong() {
        long begin = System.nanoTime();
        byteBufferWriter.readLong();
        System.out.println(System.nanoTime() - begin);
    }

    @Test
    void readLongAt() {
        long begin = System.nanoTime();
        byteBufferWriter.readLongAt(1L);
        System.out.println(System.nanoTime() - begin);
    }

    @Test
    void clear() {
        byteBufferWriter.clear();
    }
}