package cn.ttplatform.wh.core.connector.message;

import cn.ttplatform.wh.constant.MessageType;
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
        return MessageType.INSTALL_SNAPSHOT_RESULT;
    }

}
