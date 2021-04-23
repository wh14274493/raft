package cn.ttplatform.wh.core.role;

import cn.ttplatform.wh.constant.RoleType;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:14
 */
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class Follower extends AbstractRole {

    private String leaderId;
    private int preVoteCounts;
    private String voteTo;

    @Override
    public String toString() {
        return String.format("Follower{term=%d, leaderId='%s', preVoteCounts=%d, voteTo='%s'}", getTerm(), leaderId,
            preVoteCounts, voteTo);
    }

    @Override
    public RoleType getType() {
        return RoleType.FOLLOWER;
    }

    @Override
    public boolean compare(Role oldRole) {
        Follower old = (Follower) oldRole;
        return Objects.equals(voteTo, old.getVoteTo()) && Objects.equals(leaderId, old.getLeaderId())
            && preVoteCounts == old.getPreVoteCounts();
    }
}
