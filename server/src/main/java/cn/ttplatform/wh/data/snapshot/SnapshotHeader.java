package cn.ttplatform.wh.data.snapshot;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * @author Wang Hao
 * @date 2021/7/1 17:34
 */
@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SnapshotHeader {

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
        fileSize = 0;
    }
}
