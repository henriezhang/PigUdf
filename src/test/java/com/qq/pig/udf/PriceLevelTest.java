package com.qq.pig.udf;

import junit.framework.TestCase;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: antyrao
 * Date: 13-11-25
 * Time: 下午7:16
 */
public class PriceLevelTest extends TestCase
{
    @Test
    public void testGetLevel() throws Exception
    {

        PriceLevel leveler = new PriceLevel();
        assertEquals(1, leveler.getLevel(1d));
        assertEquals(2, leveler.getLevel(7d));
        assertEquals(2, leveler.getLevel(10d));
        assertEquals(3, leveler.getLevel(11d));
        assertEquals(7, leveler.getLevel(33d));
        assertEquals(9, leveler.getLevel(44d));
        assertEquals(5, leveler.getLevel(23d));
        assertEquals(16,leveler.getLevel(10000d));
    }
}
