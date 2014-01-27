package com.qq.pig.udf;

import com.google.common.base.Preconditions;
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

    /**
     * @param value
     * @return
     */
    static int roundUpToInt(Float value)
    {
        int normalizedWeight = Math.round(value * 100);
        if (normalizedWeight > 0)
            return normalizedWeight;
        Preconditions.checkState(normalizedWeight == 0);
        if (value > 0)
            return 1;
        return 0;
    }
}
