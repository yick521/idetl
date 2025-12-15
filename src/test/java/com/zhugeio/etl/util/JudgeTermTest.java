package com.zhugeio.etl.util;

import org.junit.Test;

/**
 * Created by hanqi on 18-3-2.
 */
public class JudgeTermTest {

    @Test
    public void testTerm(){

        JudgeTerm.judgeTerm();
        try{
            Thread.sleep(86400000);
        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }
}
