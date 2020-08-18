package cn.ttplatform.lc.event;

import cn.ttplatform.lc.rpc.message.AppendLogEntriesMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * @author : wang hao
 * @description : AppendLogEntriesMessageEvent
 * @date :  2020/8/16 13:00
 **/
@Getter
@Builder
@AllArgsConstructor
public class AppendLogEntriesMessageEvent implements Event {

    private AppendLogEntriesMessage message;

}
