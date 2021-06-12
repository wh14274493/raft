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

/**
 * @author Wang Hao
 * @date 2021/2/4 23:34
 */
@Setter
@Getter
public class SnapshotFile {

    public static final int HEADER_LENGTH = 16;
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
            ByteBuffer byteBuffer = byteBufferPool.allocate(HEADER_LENGTH);
            try {
                file.readBytes(0L, byteBuffer, HEADER_LENGTH);
                long contentLength = Bits.getLong(byteBuffer);
                if (contentLength != file.size() - HEADER_LENGTH) {
                    throw new SnapshotParseException("snapshot content is unmatched.");
                }
                snapshotHeader = SnapshotHeader.builder()
                    .contentLength(contentLength)
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
        ByteBuffer byteBuffer = byteBufferPool.allocate((int) snapshotHeader.getContentLength());
        try {
            file.readBytes(HEADER_LENGTH, byteBuffer, (int) snapshotHeader.getContentLength());
        } finally {
            byteBufferPool.recycle(byteBuffer);
        }
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

        private int lastIncludeIndex;
        private int lastIncludeTerm;
        private long contentLength;

        public void reset() {
            lastIncludeIndex = 0;
            lastIncludeTerm = 0;
            contentLength = 0;
        }
    }

}
