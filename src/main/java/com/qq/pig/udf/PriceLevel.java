package com.qq.pig.udf;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: antyrao
 * Date: 13-11-25
 * Time: 下午7:10
 * <p/>
 * keep a mapping from price to number which stand for a level
 */
public class PriceLevel
{
    private static final List<Long> prices;
    private static final long min;
    private static final long max;

    static
    {
        prices = Lists.newArrayList();
        prices.add(5l);//1
        prices.add(10l);//1
        prices.add(20l);//4
        prices.add(30l);//6

        min = prices.get(0);
        max = prices.get(prices.size() - 1);

    }


    public int getLevel(double price)
    {
        if (price < min || price > max)
        {
            return -1;
        }
        int level = 0;
        for (Long item : prices)
        {
            if (price <= item)
            {
                break;
            }
            level++;
        }
        return level;
    }
}
