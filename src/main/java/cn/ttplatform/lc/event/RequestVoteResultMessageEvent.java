package cn.ttplatform.lc.event;

import cn.ttplatform.lc.rpc.message.RequestVoteResultMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * @author : wang hao
 * @description : RequestVoteResultMessageEvent
 * @date :  2020/8/16 13:08
 **/
@Getter
@Builder
@AllArgsConstructor
public class RequestVoteResultMessageEvent implements Event {

    private String sourceId;
    private RequestVoteResultMessage message;
}
