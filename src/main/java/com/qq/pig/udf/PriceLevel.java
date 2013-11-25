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

    static
    {
        prices = Lists.newArrayList();
        prices.add(5l);//1
        prices.add(10l);//2
        prices.add(15l);//3
        prices.add(20l);//4
        prices.add(25l);//5
        prices.add(30l);//6
        prices.add(35l);//7
        prices.add(40l);//8
        prices.add(45l);//9
        prices.add(50l);//10
        prices.add(60l);//11
        prices.add(70l);//12
        prices.add(80l);//13
        prices.add(90l);//14
        prices.add(100l);//15
        prices.add(Long.MAX_VALUE);

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
