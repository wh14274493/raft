package cn.ttplatform.lc.node;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午10:40
 */
public interface NodeStore {

    /**
     * Write {@code term} into file, memory etc.
     *
     * @param term Current node's term
     */
    void setCurrentTerm(int term);

    /**
     * Get term from file, memory etc, when node restart
     */
    int getCurrentTerm();

    /**
     * @param voteTo if node's role is {@link cn.ttplatform.lc.node.role.Follower} then it's {@code voteTo} should be
     *               recorded， otherwise there may be a problem of repeating voting.
     */
    void setVoteTo(String voteTo);

    /**
     * Get {@code voteTo} from file, memory etc, when node restart
     */
    String getVoteTo();
}
