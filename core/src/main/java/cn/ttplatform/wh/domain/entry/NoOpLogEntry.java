package cn.ttplatform.wh.domain.entry;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午9:37
 */
@Setter
@Getter
@SuperBuilder
@NoArgsConstructor
public class NoOpLogEntry extends AbstractLogEntry {

    @Override
    public String toString() {
        return "OpEntry{type = " + this.getType() + ", term = " + this.getTerm() + ", index = " + this.getIndex() + "}";
    }

    @Override
    public byte[] getCommand() {
        return new byte[0];
    }
}
