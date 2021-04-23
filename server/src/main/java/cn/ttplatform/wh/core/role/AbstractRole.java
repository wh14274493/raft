package cn.ttplatform.wh.core.role;

import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:12
 */
@Setter
@Getter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public abstract class AbstractRole implements Role {

    private int term;
    private ScheduledFuture<?> scheduledFuture;

    @Override
    public void cancelTask() {
        this.scheduledFuture.cancel(false);
    }

    @Override
    public boolean compareState(Role oldRole) {
        if (this.term == oldRole.getTerm() && Objects.equals(this.getType(), oldRole.getType())) {
            return compare(oldRole);
        }
        return false;
    }

    /**
     * judge the state of current object whether equals oldRole or not
     *
     * @param oldRole oldRole
     * @return judgement
     */
    public abstract boolean compare(Role oldRole);
}
