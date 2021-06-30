package cn.ttplatform.wh.data.snapshot;

import cn.ttplatform.wh.data.support.SyncFileOperator;
import cn.ttplatform.wh.exception.SnapshotParseException;
import cn.ttplatform.wh.support.Pool;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.ByteBuffer;

import static cn.ttplatform.wh.data.snapshot.Snapshot.SnapshotHeader.*;

/**
 * @author Wang Hao
 * @date 2021/2/4 23:34
 */
@Setter
@Getter
@Slf4j
public class Snapshot {

    private final SyncFileOperator fileOperator;
    private SnapshotHeader snapshotHeader;
    private final Pool<ByteBuffer> byteBufferPool;

    public Snapshot(File file, Pool<ByteBuffer> byteBufferPool) {
        this.fileOperator = new SyncFileOperator(file, byteBufferPool, SnapshotHeader.BYTES);
        this.byteBufferPool = byteBufferPool;
        initialize();
    }

    public void initialize() {
        if (!fileOperator.isEmpty()) {
            long fileSize = fileOperator.readLongFromHeader(FILE_SIZE_FIELD_POSITION);
            if (fileSize > fileOperator.size()) {
                throw new SnapshotParseException("read an incomplete log snapshot file.");
            }
            snapshotHeader = SnapshotHeader.builder()
                    .fileSize(fileSize)
                    .lastIncludeIndex(fileOperator.readIntFromHeader(LAST_INCLUDE_INDEX_FIELD_POSITION))
                    .lastIncludeTerm(fileOperator.readIntFromHeader(LAST_INCLUDE_TERM_FIELD_POSITION))
                    .build();
        } else {
            snapshotHeader = new SnapshotHeader();
        }
    }

    public int getLastIncludeIndex() {
        return snapshotHeader.getLastIncludeIndex();
    }

    public int getLastIncludeTerm() {
        return snapshotHeader.getLastIncludeTerm();
    }

    public byte[] read(long offset, int length) {
        if (fileOperator.isEmpty()) {
            log.debug("snapshot file is empty.");
            return new byte[0];
        }
        long fileSize = fileOperator.size();
        if (offset > fileSize) {
            throw new IllegalStateException("offset[" + offset + "] out of bound[" + fileSize + "]");
        }
        length = Math.min(length, (int) (fileSize - offset));
        return fileOperator.readBytes(offset, length);
    }

    public ByteBuffer read() {
        ByteBuffer byteBuffer = byteBufferPool.allocate(snapshotHeader.getContentLength());
        fileOperator.readBytes(SnapshotHeader.BYTES, byteBuffer, snapshotHeader.getContentLength());
        return byteBuffer;
    }

    public boolean isEmpty() {
        return fileOperator.isEmpty();
    }

    public long size() {
        return fileOperator.size();
    }

    public void clear() {
        fileOperator.truncate(SnapshotHeader.BYTES);
        snapshotHeader.reset();
    }

    public void close() {
        fileOperator.close();
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
        public static final long FILE_SIZE_FIELD_POSITION = 0L;
        public static final long LAST_INCLUDE_INDEX_FIELD_POSITION = 8L;
        public static final long LAST_INCLUDE_TERM_FIELD_POSITION = 12L;
        private long fileSize;
        private int lastIncludeIndex;
        private int lastIncludeTerm;

        public void reset() {
            lastIncludeIndex = 0;
            lastIncludeTerm = 0;
            fileSize = BYTES;
        }

        public int getContentLength() {
            return (int) (fileSize - BYTES);
        }

    }

}
