package cn.ttplatform.lc.event;

import cn.ttplatform.lc.rpc.message.AppendLogEntriesMessage;
import cn.ttplatform.lc.rpc.message.AppendLogEntriesResultMessage;
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
public class AppendLogEntriesResultMessageEvent implements Event {

    private String sourceId;
    private AppendLogEntriesResultMessage message;
    private AppendLogEntriesMessage LastAppendLogEntriesMessage;
}
