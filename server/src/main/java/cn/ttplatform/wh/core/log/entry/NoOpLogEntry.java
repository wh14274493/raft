package cn.ttplatform.wh.core.log.entry;

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
    public byte[] getCommand() {
        return new byte[0];
    }

    @Override
    public int getCommandLength() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "NoOpLogEntry{metadata=" + metadata + '}';
    }
}
