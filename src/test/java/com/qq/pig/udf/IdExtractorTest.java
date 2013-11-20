package com.qq.pig.udf;

import com.google.common.collect.Lists;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: antyrao
 * Date: 13-11-12
 * Time: 下午6:05
 */
public class IdExtractorTest extends TestCase
{


    public void equal(List<String> expected, List<String> acutal)
    {
        assertEquals(expected.size(), acutal.size());
        for (int i = 0; i < expected.size(); i++)
        {
            assertEquals(expected.get(i), acutal.get(i));
        }
    }



    @Test
    public void testExtractId() throws Exception
    {
        IdExtractor extractor = new IdExtractor();
        List<String> testStrs = Lists.newArrayList();
        String url;
        url="http://data.auto.qq.com/car_serial/220/index.shtml";
        equal(Lists.newArrayList("220"), extractor.extractId(url));
        url="http://data.auto.qq.com/car_serial/220/modelscompare.shtml";
        equal(Lists.newArrayList("220"), extractor.extractId(url));
        url="http://data.auto.qq.com/car_serial/220/serialpic_nl.shtml";
        equal(Lists.newArrayList("220"), extractor.extractId(url));
        url="http://data.auto.qq.com/car_serial/220/car_detail_tag25.shtml";
        equal(Lists.newArrayList("220"), extractor.extractId(url));
        url="http://data.auto.qq.com/car_serial/220/news_sj.shtml";
        equal(Lists.newArrayList("220"), extractor.extractId(url));
        url="http://data.auto.qq.com/car_serial/220/news_dg.shtml";
        equal(Lists.newArrayList("220"), extractor.extractId(url));
        url="http://data.auto.qq.com/car_serial/220/news_hq.shtml";
        equal(Lists.newArrayList("220"), extractor.extractId(url));
        url="http://data.auto.qq.com/car_serial/220/fuelcost.shtml";
        equal(Lists.newArrayList("220"), extractor.extractId(url));
        url="http://data.auto.qq.com/car_serial/572/hq/hangqing_serial.shtml\n";
        equal(Lists.newArrayList("572"), extractor.extractId(url));
        url="http://data.auto.qq.com/car_evaluat/566/index_137.shtml\n";
        equal(Lists.newArrayList("566"), extractor.extractId(url));
        url="http://data.auto.qq.com/car_purchases/569/index.shtml\n";
        equal(Lists.newArrayList("569"), extractor.extractId(url));

        url="http://data.auto.qq.com/car_models/16445/index.shtml\n";
        equal(Lists.newArrayList("16445"), extractor.extractId(url));

        url="http://data.auto.qq.com/car_video/986/serialvideos.shtml#1\n";
        equal(Lists.newArrayList("986"), extractor.extractId(url));

        url="http://cgi.data.auto.qq.com/php/index.php?mod=wom&serial=220";
        equal(Lists.newArrayList("220"), extractor.extractId(url));

        url="http://cgi.data.auto.qq.com/php/search.php?brand_id=2";
        equal(Lists.newArrayList("2"), extractor.extractId(url));


        url="http://data.auto.qq.com/car_public/1/disp_pic_nl.shtml#sid=214&tid=1&pid=1070857\n";
        equal(Lists.newArrayList("214"), extractor.extractId(url));

        url="http://data.auto.qq.com/car_public/1/compare.shtml#cmpstr=15128,0,0,0,0\n";

        equal(Lists.newArrayList("15128"), extractor.extractId(url));
        url="http://data.auto.qq.com/piclib/index.shtml#sid=212\n";
        equal(Lists.newArrayList("212"), extractor.extractId(url));


    }
}
