package cn.ttplatform.lc.event;

import cn.ttplatform.lc.rpc.message.InstallSnapshotMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * @author : wang hao
 * @description : AppendLogEntriesResultMessageEvent
 * @date :  2020/8/16 13:04
 **/
@Getter
@Builder
@AllArgsConstructor
public class InstallSnapshotMessageEvent implements Event {

    private InstallSnapshotMessage message;
}
