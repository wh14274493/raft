package cn.ttplatform.wh.role;

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
    private long lastHeartBeat;

    public int getNewConfigPreVoteCounts() {
        return 0xffff & (preVoteCounts >>> 16);
    }

    public int getOldConfigPreVoteCounts() {
        return 0xffff & preVoteCounts;
    }

    public int incrementNewCountsAndGet() {
        preVoteCounts += (1 << 16);
        return getNewConfigPreVoteCounts();
    }

    public int incrementOldCountsAndGet() {
        preVoteCounts += 1;
        return getOldConfigPreVoteCounts();
    }

    @Override
    public RoleType getType() {
        return RoleType.FOLLOWER;
    }

    @Override
    public String toString() {
        return String.format("Follower{term=%d, leaderId='%s', preVoteCounts=%d, voteTo='%s'}", getTerm(), leaderId,
            preVoteCounts, voteTo);
    }

    @Override
    public boolean compare(Role oldRole) {
        Follower old = (Follower) oldRole;
        return Objects.equals(voteTo, old.getVoteTo()) && Objects.equals(leaderId, old.getLeaderId())
            && preVoteCounts == old.getPreVoteCounts();
    }

}
