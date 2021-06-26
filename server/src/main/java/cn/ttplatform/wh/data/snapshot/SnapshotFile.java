package cn.ttplatform.wh.data.snapshot;

import cn.ttplatform.wh.data.tool.Bits;
import cn.ttplatform.wh.data.tool.ByteBufferWriter;
import cn.ttplatform.wh.data.tool.ReadableAndWriteableFile;
import cn.ttplatform.wh.exception.SnapshotParseException;
import cn.ttplatform.wh.support.Pool;

import java.io.File;
import java.nio.ByteBuffer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/4 23:34
 */
@Setter
@Getter
@Slf4j
public class SnapshotFile {

    private final ReadableAndWriteableFile file;
    private SnapshotHeader snapshotHeader;
    private final Pool<ByteBuffer> byteBufferPool;

    public SnapshotFile(File file, Pool<ByteBuffer> byteBufferPool) {
        this.file = new ByteBufferWriter(file, byteBufferPool);
        this.byteBufferPool = byteBufferPool;
        initialize();
    }

    public void initialize() {
        if (!file.isEmpty()) {
            ByteBuffer byteBuffer = byteBufferPool.allocate(SnapshotHeader.BYTES);
            try {
                file.readBytes(0L, byteBuffer, SnapshotHeader.BYTES);
                byteBuffer.position(0);
                long fileSize = Bits.getLong(byteBuffer);
                if (fileSize > file.size()) {
                    throw new SnapshotParseException("read an incomplete log snapshot file.");
                }
                snapshotHeader = SnapshotHeader.builder()
                        .fileSize(fileSize)
                        .lastIncludeIndex(Bits.getInt(byteBuffer))
                        .lastIncludeTerm(Bits.getInt(byteBuffer))
                        .build();
            } finally {
                byteBufferPool.recycle(byteBuffer);
            }
        } else {
            snapshotHeader = new SnapshotHeader();
        }
    }

    public byte[] read(long offset, int size) {
        return file.readBytes(offset, size);
    }

    public ByteBuffer readAll() {
        ByteBuffer byteBuffer = byteBufferPool.allocate(snapshotHeader.getContentLength());
        file.readBytes(SnapshotHeader.BYTES, byteBuffer, snapshotHeader.getContentLength());
        return byteBuffer;
    }

    public boolean isEmpty() {
        return file.isEmpty();
    }

    public long size() {
        return file.size();
    }

    public void delete() {
        file.delete();
    }

    public void clear() {
        file.clear();
    }

    public void close() {
        file.close();
    }

    @Getter
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    static class SnapshotHeader {

        /**
         * fileSize(8 bytes) + lastIncludeIndex(4 bytes) + lastIncludeTerm(4 bytes) = 16
         */
        public static final int BYTES = 8 + 4 + 4;
        private int lastIncludeIndex;
        private int lastIncludeTerm;
        private long fileSize;

        public void reset() {
            lastIncludeIndex = 0;
            lastIncludeTerm = 0;
            fileSize = 0;
        }

        public int getContentLength() {
            return (int) (fileSize - BYTES);
        }
    }

}
