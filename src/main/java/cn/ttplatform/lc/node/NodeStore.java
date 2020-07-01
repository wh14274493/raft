package cn.ttplatform.lc.node;

import java.io.FileNotFoundException;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午10:40
 */
public interface NodeStore {

    default FileNodeStore newFileNodeStore() throws FileNotFoundException {
        return new FileNodeStore();
    }

    void setCurrentTerm(int term);

    int getCurrentTerm();

    void setVoteTo(String voteTo);

    String getVoteTo();
}
