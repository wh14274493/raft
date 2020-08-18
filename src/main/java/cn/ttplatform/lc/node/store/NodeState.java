package cn.ttplatform.lc.node.store;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午10:40
 */
public interface NodeState {

    /**
     * Write {@code term} into file, memory etc.
     *
     * @param term Current node's term
     */
    void setCurrentTerm(int term);

    /**
     * Get term from file, memory etc, when node restart
     *
     * @return currentTerm
     */
    int getCurrentTerm();

    /**
     * if node's role is {@link cn.ttplatform.lc.node.role.Follower} then it's {@code voteTo} should be recorded， otherwise there may be a problem of repeating voting.
     *
     * @param voteTo the node id that vote for
     */
    void setVoteTo(String voteTo);

    /**
     * Get {@code voteTo} from file, memory etc, when node restart
     *
     * @return the node id that vote for
     */
    String getVoteTo();
}
