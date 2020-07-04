package cn.ttplatform.lc.node;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.Charset;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午10:43
 */
public class FileNodeStore implements NodeStore {

    private static final long TERM_OFFSET = 0L;
    private static final long VOTE_TO_OFFSET = Integer.BYTES;
    private final RandomAccessFile storeFile;
    private final FileChannel channel;

    public FileNodeStore(File file) {
        try {
            this.storeFile = new RandomAccessFile(file, "rw");
            channel = storeFile.getChannel();
        } catch (FileNotFoundException e) {
            throw new IllegalStateException("File(" + file.getPath() + ") are not found");
        }
    }

    @Override
    public void setCurrentTerm(int term) {
        try {
            MappedByteBuffer map = channel.map(MapMode.READ_WRITE, TERM_OFFSET, Integer.BYTES);
            map.putInt(term);
        } catch (IOException e) {
            throw new IllegalStateException("Write [term(" + term + ")] into file error");
        }
    }

    @Override
    public int getCurrentTerm() {
        if (hasContent()) {
            try {
                MappedByteBuffer map = channel.map(MapMode.READ_ONLY, TERM_OFFSET, Integer.BYTES);
                return map.getInt();
            } catch (IOException e) {
                throw new IllegalStateException("Read [term] from file error");
            }
        }
        return 0;
    }

    @Override
    public void setVoteTo(String voteTo) {
        try {
            byte[] bytes = voteTo.getBytes(Charset.defaultCharset());
            int length = bytes.length;
            MappedByteBuffer map = channel.map(MapMode.READ_WRITE, VOTE_TO_OFFSET, length + Integer.BYTES);
            map.putInt(length);
            map.put(bytes);
        } catch (IOException e) {
            throw new IllegalStateException("Write [voteTo(" + voteTo + ")] into file error");
        }
    }

    @Override
    public String getVoteTo() {
        if (hasContent()) {
            try {
                storeFile.seek(VOTE_TO_OFFSET);
                int length = storeFile.readInt();
                byte[] voteToBytes = new byte[length];
                storeFile.read(voteToBytes);
                return new String(voteToBytes, Charset.defaultCharset());
            } catch (IOException e) {
                throw new IllegalStateException("Read [voteTo] from file error");
            }
        }
        return null;
    }

    private boolean hasContent() {
        try {
            return storeFile.length() > 2 * Integer.BYTES;
        } catch (IOException e) {
            throw new IllegalStateException("Get file size error");
        }
    }
}
