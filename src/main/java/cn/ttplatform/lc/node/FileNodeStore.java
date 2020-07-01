package cn.ttplatform.lc.node;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午10:43
 */
public class FileNodeStore implements NodeStore {


    private static final String STORE_FILE_PATH = "node.store";
    private static final int TERM_OFFSET = 0;
    private static final int VOTE_TO_OFFSET = 4;


    private final RandomAccessFile storeFile;

    public FileNodeStore() {
        try {
            this.storeFile = new RandomAccessFile(STORE_FILE_PATH, "rw");
        } catch (FileNotFoundException e) {
            throw new RuntimeException("");
        }
    }

    @Override
    public void setCurrentTerm(int term) {
        try {
            storeFile.seek(TERM_OFFSET);
            storeFile.writeInt(term);
        } catch (IOException e) {
            throw new RuntimeException("");
        }
    }

    @Override
    public int getCurrentTerm() {
        int term;
        try {
            storeFile.seek(TERM_OFFSET);
            term = storeFile.readInt();
        } catch (IOException e) {
            throw new RuntimeException("");
        }
        return term;
    }

    @Override
    public void setVoteTo(String voteTo) {
        try {
            storeFile.seek(VOTE_TO_OFFSET);
            storeFile.writeBytes(voteTo);
        } catch (IOException e) {
            throw new RuntimeException("");
        }
    }

    @Override
    public String getVoteTo() {
        String voteTo;
        try {
            storeFile.seek(VOTE_TO_OFFSET);
            voteTo = storeFile.readUTF();
        } catch (IOException e) {
            throw new RuntimeException("");
        }
        return voteTo;
    }
}
