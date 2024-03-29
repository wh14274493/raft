package cn.ttplatform.wh.message;

import cn.ttplatform.wh.constant.DistributableType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/**
 * @author : Wang Hao
 * @date :  2020/8/16 17:05
 **/
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class InstallSnapshotResultMessage extends AbstractMessage{

    private int term;
    private boolean done;
    private long offset;
    private boolean success;

    @Override
    public int getType() {
        return DistributableType.INSTALL_SNAPSHOT_RESULT;
    }

    @Override
    public String toString() {
        return "InstallSnapshotResultMessage{" +
            "term=" + term +
            ", done=" + done +
            ", offset=" + offset +
            ", success=" + success +
            '}';
    }
}
