package com.qq.pig.udf;

import junit.framework.TestCase;
import org.junit.Test;

/**
 * Created by antyrao on 14-3-5.
 */
public class PriceRangeTest extends TestCase
{
    @Test
    public void testGetLevel() throws Exception
    {
        PriceRange range = new PriceRange();
        assertEquals(1, range.getLevel(7));
        assertEquals(1, range.getLevel(1));
        assertEquals(2, range.getLevel(8));
        assertEquals(2,range.getLevel(10));
        assertEquals(8,range.getLevel(300));
    }
}
