package com.qq.pig.udf;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by antyrao on 14-3-5.
 */
public class PriceRange
{

    private static final List<Long> prices;

    //TODO make price range dictionary exchangeable
    static
    {
        //0-7w,7-10w,10-15w,15-22w,22-35w,35-50w,50-100w,100-300w,300w以上
        prices = Lists.newArrayList();
        prices.add(7l);//1
        prices.add(10l);//2
        prices.add(15l);//3
        prices.add(22l);//4
        prices.add(35l);//5
        prices.add(50l);//6
        prices.add(100l);//7
        prices.add(300l);//8
        prices.add(Long.MAX_VALUE);//9
    }

    public int getLevel(double price)
    {
        int level = 0;
        for (Long item : prices)
        {
            if (price <= item)
            {
                break;
            }

            level++;
        }
        return level + 1;
    }
}
