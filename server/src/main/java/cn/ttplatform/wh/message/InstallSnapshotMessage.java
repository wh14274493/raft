package cn.ttplatform.wh.message;

import cn.ttplatform.wh.constant.DistributableType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/**
 * @author : wang hao
 * @date :  2020/8/16 13:17
 **/
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class InstallSnapshotMessage extends AbstractMessage {

    private int term;
    private int lastIncludeIndex;
    private int lastIncludeTerm;
    private long offset;
    private byte[] chunk;
    private boolean done;

    @Override
    public int getType() {
        return DistributableType.INSTALL_SNAPSHOT;
    }

    @Override
    public String toString() {
        return "InstallSnapshotMessage{" +
            "term=" + term +
            ", lastIncludeIndex=" + lastIncludeIndex +
            ", lastIncludeTerm=" + lastIncludeTerm +
            ", offset=" + offset +
            ", chunkLength=" + (chunk == null ? 0 : chunk.length) +
            ", done=" + done +
            '}';
    }
}
