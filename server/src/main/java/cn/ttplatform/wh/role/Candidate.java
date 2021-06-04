package cn.ttplatform.wh.role;

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

    public int getNewConfigVoteCounts() {
        return 0xffff & (voteCounts >>> 16);
    }

    public int getOldConfigVoteCounts() {
        return 0xffff & voteCounts;
    }

    public int incrementNewCountsAndGet() {
        voteCounts += (1 << 16);
        return getNewConfigVoteCounts();
    }

    public int incrementOldCountsAndGet() {
        voteCounts += 1;
        return getOldConfigVoteCounts();
    }

    @Override
    public String toString() {
        return String.format("Candidate{term=%d, voteCounts=%d}", getTerm(), getVoteCounts());
    }

    @Override
    public RoleType getType() {
        return RoleType.CANDIDATE;
    }
}
