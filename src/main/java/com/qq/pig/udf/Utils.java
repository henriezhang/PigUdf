package com.qq.pig.udf;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * Created with IntelliJ IDEA.
 * User: antyrao
 * Date: 13-12-23
 * Time: 上午10:22
 */
public class Utils
{
    public static void populateWithZero(Tuple tuple) throws ExecException
    {
        int size = tuple.size();
        for (int i = 0; i < size; i++)
        {
            tuple.set(i, 0);
        }
    }
}
