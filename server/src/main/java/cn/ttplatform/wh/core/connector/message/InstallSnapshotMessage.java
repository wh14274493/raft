package cn.ttplatform.wh.core.connector.message;

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

}
