package cn.ttplatform.lc.entry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:12
 */
public class EntryFile {

    private static final EntryFactory ENTRYFACTORY = EntryFactory.INSTANCE;
    private static final long FIXED_LENGTH = 1L << 4;

    private final RandomAccessFile file;
    private final FileChannel channel;

    public EntryFile(RandomAccessFile file) {
        this.file = file;
        this.channel = this.file.getChannel();
    }

    public long appendEntry(Entry entry) throws IOException {
        long offset = file.length();
        long size = FIXED_LENGTH + entry.getCommand().length;
        MappedByteBuffer mappedByteBuffer = channel.map(MapMode.READ_WRITE, offset, size);
        mappedByteBuffer.putInt(entry.getType());
        mappedByteBuffer.putInt(entry.getTerm());
        mappedByteBuffer.putInt(entry.getIndex());
        mappedByteBuffer.putInt(entry.getCommand().length);
        mappedByteBuffer.put(entry.getCommand());
        return offset;
    }

    public Entry loadEntry(long offset) {
        try {
            long size = file.length();
            if (offset > size) {
                throw new IllegalArgumentException("offset(" + offset + ") is not correct, and file size is " + size);
            }
            file.seek(offset);
            int type = file.readInt();
            int term = file.readInt();
            int index = file.readInt();
            int commandSize = file.readInt();
            byte[] command = new byte[commandSize];
            file.read(command);
            return ENTRYFACTORY.createEntry(type, term, index, command);
        } catch (IOException e) {
            throw new IllegalStateException("Read file error");
        }

    }

    public void clear() throws IOException {
        file.setLength(0L);
    }

    public void close() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
        if (file != null) {
            file.close();
        }
    }

}
