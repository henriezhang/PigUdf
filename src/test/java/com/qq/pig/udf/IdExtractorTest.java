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


    public void equal(List<List<String>> expected, List<List<String>> acutal)
    {

        assertEquals(expected.size(), acutal.size());
        for (int i = 0; i < expected.size(); i++)
        {
            equal2(expected.get(i), acutal.get(i));
        }
    }

    public void equal2(List<String> expected, List<String> acutal)
    {
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
        List<String> expected = Lists.newArrayList();
        List<List<String>> result = Lists.newArrayList();
        String url;
        url = "http://data.auto.qq.com/car_serial/220/index.shtml";
        init(expected);
        expected.set(1, "220");
        result.add(expected);
        equal(result, extractor.extractId(url));
        url = "http://data.auto.qq.com/car_serial/220/modelscompare.shtml";
        init(expected);
        expected.set(1, "220");
        result.clear();
        result.add(expected);
        equal(result, extractor.extractId(url));
        url = "http://data.auto.qq.com/car_serial/220/serialpic_nl.shtml";
        init(expected);
        expected.set(1, "220");
        result.clear();

        result.add(expected);
        equal(result, extractor.extractId(url));
        url = "http://data.auto.qq.com/car_serial/220/car_detail_tag25.shtml";
        init(expected);
        expected.set(1, "220");
        result.clear();
        result.add(expected);
        equal(result, extractor.extractId(url));
        url = "http://data.auto.qq.com/car_serial/220/news_sj.shtml";
        init(expected);
        expected.set(1, "220");
        result.clear();
        result.add(expected);
        equal(result, extractor.extractId(url));
        url = "http://data.auto.qq.com/car_serial/220/news_dg.shtml";
        init(expected);
        expected.set(1, "220");
        result.clear();
        result.add(expected);
        equal(result, extractor.extractId(url));
        url = "http://data.auto.qq.com/car_serial/220/news_hq.shtml";
        init(expected);
        expected.set(1, "220");
        result.clear();
        result.add(expected);
        equal(result, extractor.extractId(url));
        url = "http://data.auto.qq.com/car_serial/220/fuelcost.shtml";
        init(expected);
        expected.set(1, "220");
        result.clear();
        result.add(expected);
        equal(result, extractor.extractId(url));
        url = "http://data.auto.qq.com/car_serial/572/hq/hangqing_serial.shtml\n";
        init(expected);
        expected.set(1, "572");
        result.clear();
        result.add(expected);
        equal(result, extractor.extractId(url));
        url = "http://data.auto.qq.com/car_evaluat/566/index_137.shtml\n";
        init(expected);
        expected.set(1, "566");
        result.clear();
        result.add(expected);
        equal(result, extractor.extractId(url));
        url = "http://data.auto.qq.com/car_purchases/569/index.shtml\n";
        init(expected);
        expected.set(1, "569");
        result.clear();
        result.add(expected);
        equal(result, extractor.extractId(url));

        url = "http://data.auto.qq.com/car_models/16445/index.shtml\n";
        init(expected);
        expected.set(2, "16445");
        result.clear();
        result.add(expected);
        equal(result, extractor.extractId(url));

        url = "http://data.auto.qq.com/car_video/986/serialvideos.shtml#1\n";
        init(expected);
        expected.set(1, "986");
        result.clear();
        result.add(expected);
        equal(result, extractor.extractId(url));

        url = "http://cgi.data.auto.qq.com/php/index.php?mod=wom&serial=220";
        init(expected);
        expected.set(1, "220");
        result.clear();
        result.add(expected);
        equal(result, extractor.extractId(url));

        url = "http://cgi.data.auto.qq.com/php/search.php?brand_id=2";
        init(expected);
        expected.set(0, "2");
        result.clear();
        result.add(expected);
        equal(result, extractor.extractId(url));


        url = "http://data.auto.qq.com/car_public/1/disp_pic_nl.shtml#sid=214&tid=1&pid=1070857\n";
        init(expected);
        expected.set(1, "214");
        result.clear();
        result.add(expected);
        equal(result, extractor.extractId(url));

        url = "http://data.auto.qq.com/car_public/1/compare.shtml#cmpstr=15128,0,0,0,0\n";

        init(expected);
        expected.set(2, "15128");
        result.clear();
        result.add(expected);
        equal(result, extractor.extractId(url));
        url = "http://data.auto.qq.com/piclib/index.shtml#sid=212\n";
        init(expected);
        expected.set(1, "212");
        result.clear();
        result.add(expected);
        equal(result, extractor.extractId(url));


    }

    private void init(List<String> expected)
    {
        expected.clear();
        expected.add(null);
        expected.add(null);
        expected.add(null);
    }
}
