package cn.ttplatform.wh.core;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import cn.ttplatform.wh.exception.OperateFileException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Wang Hao
 * @date 2021/5/27 17:16
 */
@Slf4j
public class FileTest {

    Path path1;
    Path path2;
    FileChannel readChannel;
    FileChannel writeChannel;
    FileChannel appendChannel;
    RandomAccessFile randomAccessFile;

    @Before
    public void setup() throws URISyntaxException, IOException {
        String property = System.getProperty("user.home");
        File file1 = File.createTempFile(property, "test1.txt");
        File file2 = File.createTempFile(property, "test2.txt");

        path1 = file1.toPath();
        path2 = file2.toPath();

        randomAccessFile = new RandomAccessFile(path1.toFile(), "rw");
        readChannel = FileChannel.open(path2, READ);
        appendChannel = FileChannel.open(path2, WRITE, APPEND);
        writeChannel = FileChannel.open(path2, WRITE);
    }

    @After
    public void after() throws IOException {
        readChannel.close();
        writeChannel.close();
        randomAccessFile.close();
    }

    @Test
    public void testRead() throws IOException {
        testWrite();
        int count = 100000;
        byte[] content = new byte[count];
        randomAccessFile.seek(0L);
        long begin = System.nanoTime();
        int read2 = randomAccessFile.read(content);
        log.info("read {} bytes with RandomAccessFile cost {} ns.", read2, System.nanoTime() - begin);

        ByteBuffer byteBuffer = ByteBuffer.wrap(content);
        byteBuffer.clear();
        begin = System.nanoTime();
        int read1 = readChannel.read(byteBuffer);
        log.info("read {} bytes with ByteBuffer cost {} ns.", read1, System.nanoTime() - begin);

        ByteBuffer direct = ByteBuffer.allocateDirect(count);
        begin = System.nanoTime();
        int read = readChannel.read(direct);
        log.info("read {} bytes with DirectByteBuffer cost {} ns.", read, System.nanoTime() - begin);
    }

    @Test
    public void testWrite1() throws IOException {
        int count = 500000;
        byte[] content = new byte[count];

        log.info("file size is {}.", writeChannel.size());

        long offset = 0L;
        byte a = 1;
        long begin = System.nanoTime();
        while (offset < count * 10) {
            for (int i = 0; i < count; i++) {
                content[i] = a;
            }
            ByteBuffer byteBuffer = ByteBuffer.wrap(content);
            appendChannel.write(byteBuffer);
            offset += count;
            a++;
        }
        log.info("write {} bytes cost {} ns.", count * 10, System.nanoTime() - begin);
        log.info("file size is {}.", writeChannel.size());

        a = 1;
        begin = System.nanoTime();
        while (offset < count * 20) {
            for (int i = 0; i < count; i++) {
                content[i] = a;
            }
            ByteBuffer byteBuffer = ByteBuffer.wrap(content);
            writeChannel.write(byteBuffer, offset);
            offset += count;
            a++;
        }
        log.info("write {} bytes cost {} ns.", count * 10, System.nanoTime() - begin);
        log.info("file size is {}.", appendChannel.size());
    }

    @Test
    public void testWrite() throws IOException {
        int count = 4*1024*1024;
        byte[] content = new byte[count];
        IntStream.range(0, count).forEach(index -> content[index] = 'a');

        long begin = System.nanoTime();
        randomAccessFile.write(content);
        log.info("write {} bytes with RandomAccessFile cost {} ns.", count, System.nanoTime() - begin);

        ByteBuffer byteBuffer = ByteBuffer.wrap(content);
        begin = System.nanoTime();
        writeChannel.write(byteBuffer);
        log.info("write {} bytes with ByteBuffer cost {} ns.", count, System.nanoTime() - begin);

        ByteBuffer direct = ByteBuffer.allocateDirect(count);
        IntStream.range(0, count).forEach(index -> direct.put((byte) 'a'));
        direct.flip();
        begin = System.nanoTime();
        writeChannel.write(direct);
        log.info("write {} bytes with DirectByteBuffer cost {} ns.", count, System.nanoTime() - begin);
    }

    @Test
    public void testReadAndWrite() throws IOException {
        int count = 100000;
        byte[] content = new byte[count];
        IntStream.range(0, count).forEach(index -> content[index] = 'a');

        ByteBuffer byteBuffer = ByteBuffer.wrap(content);
        long begin = System.nanoTime();
        writeChannel.write(byteBuffer);
        log.info("write {} bytes with ByteBuffer cost {} ns.", count, System.nanoTime() - begin);

        writeChannel.truncate(1L);

        byteBuffer.clear();
        begin = System.nanoTime();
        int read = readChannel.read(byteBuffer);
        log.info("read {} bytes with ByteBuffer cost {} ns.", read, System.nanoTime() - begin);
    }

    @Test
    public void testFileSize() throws IOException {
        int size = 100000;
        writeChannel.position(size);
        ByteBuffer byteBuffer = ByteBuffer.allocate(1);
        byteBuffer.put((byte) 1);
        byteBuffer.flip();
        writeChannel.write(byteBuffer);
        log.info("file size is {}.", writeChannel.size());

        byte a = 1;
        long begin = System.nanoTime();
        ByteBuffer buffer = ByteBuffer.allocate(size / 2);
        IntStream.range(0, size / 2).forEach(index -> buffer.put(a));
        writeChannel.position(0);
        buffer.flip();
        writeChannel.write(buffer);
        ByteBuffer buffer1 = ByteBuffer.allocate(size / 2);
        IntStream.range(0, size / 2).forEach(index -> buffer1.put(a));
        buffer1.flip();
        writeChannel.write(buffer1);
        log.info("write {} bytes with ByteBuffer cost {} ns.", size, System.nanoTime() - begin);
    }

    @Test
    public void testFileSize1() throws IOException {
        int size = 100000;
        log.info("file size is {}.", writeChannel.size());

        byte a = 1;
        long begin = System.nanoTime();
        ByteBuffer buffer1 = ByteBuffer.allocate(size / 2);
        IntStream.range(0, size / 2).forEach(index -> buffer1.put(a));
        writeChannel.position(0);
        buffer1.flip();
        writeChannel.write(buffer1);
        ByteBuffer buffer2 = ByteBuffer.allocate(size / 2);
        IntStream.range(0, size / 2).forEach(index -> buffer2.put(a));
        buffer2.flip();
        writeChannel.write(buffer2);
        log.info("write {} bytes with ByteBuffer cost {} ns.", size, System.nanoTime() - begin);
    }

    @Test
    public void testFileSize2() throws IOException {
        log.info("file size is {}.", writeChannel.size());
        Assert.assertEquals(0, writeChannel.size());
        ByteBuffer buffer = ByteBuffer.allocateDirect(4 * 1024);
        writeChannel.write(buffer);
        log.info("file size is {}.", writeChannel.size());
        Assert.assertEquals(4 * 1024, writeChannel.size());
    }

    @Test
    public void testByteBuffer() throws IOException {
        int count = 8;
        String property = System.getProperty("user.home");
        FileChannel fileChannel = FileChannel.open(File.createTempFile(property, "t2.txt").toPath(), READ, WRITE, CREATE);
        FileChannel fileChannel1 = FileChannel.open(File.createTempFile(property, "t1.txt").toPath(), READ, WRITE, CREATE);
        MappedByteBuffer mappedByteBuffer;
        try {
            mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, count);
        } catch (IOException e) {
            throw new OperateFileException("mapped file error.", e);
        }
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(count);
        long begin = System.nanoTime();
        while (mappedByteBuffer.hasRemaining()) {
            mappedByteBuffer.putInt(ThreadLocalRandom.current().nextInt());
        }
        log.info("MappedByteBuffer {} bytes cost {} ns.", count, System.nanoTime() - begin);

        while (byteBuffer.hasRemaining()) {
            byteBuffer.putInt(ThreadLocalRandom.current().nextInt());
        }
        byteBuffer.flip();
        begin = System.nanoTime();
        fileChannel1.write(byteBuffer);
        log.info("DirectByteBuffer {} bytes cost {} ns.", count, System.nanoTime() - begin);

        mappedByteBuffer.clear();
        begin = System.nanoTime();
        while (mappedByteBuffer.hasRemaining()) {
            mappedByteBuffer.putInt(ThreadLocalRandom.current().nextInt());
        }
        log.info("MappedByteBuffer {} bytes cost {} ns.", count, System.nanoTime() - begin);

        byteBuffer.clear();
        while (byteBuffer.hasRemaining()) {
            byteBuffer.putInt(ThreadLocalRandom.current().nextInt());
        }
        byteBuffer.flip();
        begin = System.nanoTime();
        fileChannel1.write(byteBuffer);
        log.info("DirectByteBuffer {} bytes cost {} ns.", count, System.nanoTime() - begin);

        mappedByteBuffer.clear();
        begin = System.nanoTime();
        while (mappedByteBuffer.hasRemaining()) {
            mappedByteBuffer.putInt(ThreadLocalRandom.current().nextInt());
        }
        log.info("MappedByteBuffer {} bytes cost {} ns.", count, System.nanoTime() - begin);

        byteBuffer.clear();
        while (byteBuffer.hasRemaining()) {
            byteBuffer.putInt(ThreadLocalRandom.current().nextInt());
        }
        byteBuffer.flip();
        begin = System.nanoTime();
        fileChannel1.write(byteBuffer);
        log.info("DirectByteBuffer {} bytes cost {} ns.", count, System.nanoTime() - begin);
    }
}
