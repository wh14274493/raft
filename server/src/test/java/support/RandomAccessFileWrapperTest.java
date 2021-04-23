package support;

import cn.ttplatform.wh.core.support.RandomAccessFileWrapper;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.util.stream.IntStream;

/**
 * @author Wang Hao
 * @date 2021/4/18 23:32
 */
class RandomAccessFileWrapperTest {

    File file = new File("D:\\workspace\\java\\raft\\core\\src\\test\\resources\\log.data");
    RandomAccessFileWrapper randomAccessFileWrapper = new RandomAccessFileWrapper(file);
    RandomAccessFile randomAccessFile = new RandomAccessFile(file,"rw");

    RandomAccessFileWrapperTest() throws FileNotFoundException {
    }

    @org.junit.jupiter.api.Test
    void seek() {
    }

    @org.junit.jupiter.api.Test
    void writeInt() throws IOException {
        randomAccessFileWrapper.clear();
//        long begin = System.currentTimeMillis();
//        IntStream.range(0,100000).forEach(index->{
//            randomAccessFileWrapper.writeInt(index);
//        });
//        System.out.println(System.currentTimeMillis()-begin);
        StringBuilder sb = new StringBuilder();
        IntStream.range(0,50).forEach(index->{
            sb.append(index);
        });
        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        MappedByteBuffer map = randomAccessFile.getChannel().map(MapMode.READ_WRITE, 0L, bytes.length * 100000);
        long begin = System.currentTimeMillis();
        IntStream.range(0,100000).forEach(index->{
            map.put(bytes);
        });
        System.out.println(System.currentTimeMillis()-begin);

    }

    @org.junit.jupiter.api.Test
    void writeIntAt() {
    }

    @org.junit.jupiter.api.Test
    void readInt() {
    }

    @org.junit.jupiter.api.Test
    void readIntAt() {
    }

    @org.junit.jupiter.api.Test
    void writeLong() {
    }

    @org.junit.jupiter.api.Test
    void writeLongAt() {
    }

    @org.junit.jupiter.api.Test
    void readLong() {
    }

    @org.junit.jupiter.api.Test
    void readLongAt() {
    }

    @org.junit.jupiter.api.Test
    void writeBytes() {
    }

    @org.junit.jupiter.api.Test
    void writeBytesAt() {
    }

    @org.junit.jupiter.api.Test
    void append() {
    }

    @org.junit.jupiter.api.Test
    void readBytes() {
    }

    @org.junit.jupiter.api.Test
    void readBytesAt() {
    }

    @org.junit.jupiter.api.Test
    void size() {
    }

    @org.junit.jupiter.api.Test
    void truncate() {
    }

    @org.junit.jupiter.api.Test
    void clear() {
        randomAccessFileWrapper.clear();
    }

    @org.junit.jupiter.api.Test
    void isEmpty() {
    }

    @org.junit.jupiter.api.Test
    void close() {
    }
}