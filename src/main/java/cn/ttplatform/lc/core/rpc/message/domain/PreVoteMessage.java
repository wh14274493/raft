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
 * @date 2020/10/2 下午4:06
 */
@Setter
@Getter
@ToString
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class PreVoteMessage extends AbstractMessage {

    private String nodeId;
    private int lastLogTerm;
    private int lastLogIndex;

    @Override
    public int getType() {
        return RpcMessageType.PRE_VOTE;
    }

}
