package cn.ttplatform.wh.core.support;

import java.util.UUID;

/**
 * @author Wang Hao
 * @date 2021/4/20 14:06
 */
public class UUIDGenerator implements IDGenerator {

    @Override
    public String generate() {
        return UUID.randomUUID().toString();
    }
}
