package cn.ttplatform.wh.core.role;

import cn.ttplatform.wh.constant.RoleType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:16
 */
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class Candidate extends AbstractRole {

    private int voteCounts;

    @Override
    public String toString() {
        return String.format("Candidate{term=%d, voteCounts=%d}", getTerm(), getVoteCounts());
    }

    @Override
    public RoleType getType() {
        return RoleType.CANDIDATE;
    }

    @Override
    public boolean compare(Role oldRole) {
        return false;
    }
}
