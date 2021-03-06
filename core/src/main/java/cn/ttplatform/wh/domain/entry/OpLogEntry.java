package cn.ttplatform.wh.domain.entry;

import java.nio.charset.Charset;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午9:39
 */
@Setter
@Getter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class OpLogEntry extends AbstractLogEntry {

    private byte[] command;

    @Override
    public byte[] getCommand() {
        return command;
    }

    @Override
    public String toString() {
        return "OpEntry{type = " + this.getType() + ", term = " + this.getTerm() + ", index = " + this.getIndex()
            + ", command = " + new String(command, Charset.defaultCharset()) + '}';
    }
}
