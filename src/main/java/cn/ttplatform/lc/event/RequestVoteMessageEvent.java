package cn.ttplatform.lc.event;

import cn.ttplatform.lc.rpc.message.RequestVoteMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * @author : wang hao
 * @description : RequestVoteMessageEvent
 * @date :  2020/8/16 13:06
 **/
@Getter
@Builder
@AllArgsConstructor
public class RequestVoteMessageEvent implements Event {

    private RequestVoteMessage message;
}
