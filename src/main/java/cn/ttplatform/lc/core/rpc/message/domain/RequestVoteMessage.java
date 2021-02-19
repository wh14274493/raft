package cn.ttplatform.lc.core.rpc.message.domain;

import cn.ttplatform.lc.constant.RpcMessageType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:18
 */
@Getter
@Setter
@SuperBuilder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class RequestVoteMessage extends AbstractMessage{

    private int term;
    private String candidateId;
    private int lastLogIndex;
    private int lastLogTerm;

    @Override
    public int getType() {
        return RpcMessageType.REQUEST_VOTE;
    }

}

