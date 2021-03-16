package cn.ttplatform.wh.core;

import cn.ttplatform.wh.constant.FileName;
import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.support.RandomAccessFileWrapper;
import java.io.File;
import java.nio.charset.Charset;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午10:40
 */
public class NodeState {

    private final RandomAccessFileWrapper file;

    public NodeState(File parent) {
        this.file = new RandomAccessFileWrapper(new File(parent, FileName.NODE_STATE_FILE_NAME));
    }

    /**
     * Write {@code term} into file, memory etc.
     *
     * @param term Current node's term
     */
    public void setCurrentTerm(int term) {
        file.writeIntAt(0L, term);
    }

    /**
     * Get term from file, memory etc, when node restart
     *
     * @return currentTerm
     */
    public int getCurrentTerm() {
        return file.readIntAt(0L);
    }

    /**
     * if node's role is {@link Follower} then it's {@code voteTo} should be recorded， otherwise there may be a problem
     * of repeating voting.
     *
     * @param voteTo the node id that vote for
     */
    public void setVoteTo(String voteTo) {
        byte[] content = voteTo.getBytes(Charset.defaultCharset());
        file.writeIntAt(Integer.BYTES, content.length);
        file.writeBytes(content);
    }

    /**
     * Get {@code voteTo} from file, memory etc, when node restart
     *
     * @return the node id that vote for
     */
    public String getVoteTo() {
        int size = file.readIntAt(Integer.BYTES);
        return new String(file.readBytesAt(Integer.BYTES * 2, size), Charset.defaultCharset());
    }

}
