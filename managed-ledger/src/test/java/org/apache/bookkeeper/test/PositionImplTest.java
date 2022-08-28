package org.apache.bookkeeper.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.junit.Test;

@Slf4j
public class PositionImplTest {

    @Test
    public void hello(){
        PositionImpl position = PositionImpl.get(1, 1);
        log.info("position:{}",position.getNext());
        log.info("ackSet:{}",position.getAckSet());
    }
}
