package cn.ttplatform.lc.event;

import cn.ttplatform.lc.rpc.message.InstallSnapshotMessage;
import cn.ttplatform.lc.rpc.message.InstallSnapshotResultMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * @author : Wang Hao
 * @date :  2020/8/16 17:03
 **/
@Getter
@Builder
@AllArgsConstructor
public class InstallSnapshotResultMessageEvent implements Event {

    private String sourceId;
    private InstallSnapshotResultMessage message;
    private InstallSnapshotMessage lastInstallSnapshotMessage;
}
