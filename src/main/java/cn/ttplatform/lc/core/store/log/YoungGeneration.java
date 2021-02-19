package cn.ttplatform.lc.core.store.log;

import cn.ttplatform.lc.constant.ExceptionMessage;
import cn.ttplatform.lc.exception.OperateFileException;
import java.io.File;

/**
 * @author Wang Hao
 * @date 2021/2/5 13:58
 */
public class YoungGeneration extends AbstractGeneration{

    public YoungGeneration(File file) {
        super(file);
    }

    public void write(byte[] content) {
        fileSnapshot.write(content);
    }

    public File rename(int lastIncludeIndex, int lastIncludeTerm) {
        File newFile = new File(this.file.getParent() + lastIncludeTerm + "_" + lastIncludeIndex);
        if (!this.file.renameTo(newFile)) {
            throw new OperateFileException(ExceptionMessage.RENAME_FILE_ERROR);
        }
        return newFile;
    }

}
