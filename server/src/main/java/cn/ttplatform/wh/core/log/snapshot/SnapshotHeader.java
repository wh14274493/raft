package cn.ttplatform.wh.core.log.snapshot;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * @author Wang Hao
 * @date 2021/4/19 21:06
 */
@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SnapshotHeader {

    private int lastIncludeIndex;
    private int lastIncludeTerm;
    private long size;
    private long contentLength;

    public void reset() {
        lastIncludeIndex = 0;
        lastIncludeTerm = 0;
        size = 0;
        contentLength = 0;
    }
}
