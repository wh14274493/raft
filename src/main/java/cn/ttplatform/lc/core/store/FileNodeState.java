package cn.ttplatform.lc.core.store;

import java.nio.charset.Charset;

/**
 * @author : wang hao
 * @description : FileNodeState
 * @date :  2020/8/15 22:35
 **/
public class FileNodeState implements NodeState {

    private final RandomAccessFileWrapper file;

    public FileNodeState(String nodeStateFilePath) {
        this.file = new RandomAccessFileWrapper(nodeStateFilePath);
    }

    @Override
    public void setCurrentTerm(int term) {
        file.writeIntAt(0L, term);
    }

    @Override
    public int getCurrentTerm() {
        return file.readIntAt(0L);
    }

    @Override
    public void setVoteTo(String voteTo) {
        byte[] content = voteTo.getBytes(Charset.defaultCharset());
        file.writeIntAt(Integer.BYTES, content.length);
        file.writeBytes(content);
    }

    @Override
    public String getVoteTo() {
        int size = file.readIntAt(Integer.BYTES);
        return new String(file.readBytesAt(Integer.BYTES * 2, size), Charset.defaultCharset());
    }
}
