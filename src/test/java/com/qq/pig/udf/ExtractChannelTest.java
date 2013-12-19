package com.qq.pig.udf;

import com.google.common.base.Splitter;
import junit.framework.TestCase;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: antyrao
 * Date: 13-12-11
 * Time: 下午4:15
 */
public class ExtractChannelTest extends TestCase
{
    @Test
    public void testEndIndexOfChannel() throws Exception
    {

        String channelInterest = "news:newsgn:gdxw:0.4541,news:0.2726,news:newsgn:0.2762,zj:news:0.7541,zj:0.1662";
        Iterable<String> iterable = Splitter.on(",").split(channelInterest);
        for (String channel : iterable)
        {
            int index = ExtractChannel2.endIndexOfChannel(channel);
//            System.out.println(channel + ":" + index);
            if (index > 0)
            {
                System.out.println(channel.substring(0, index));

                System.out.println(ExtractChannel2.getWeight(channel, index));

            }
        }


    }
}
