package com.qq.pig.udf;

import junit.framework.TestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Created by antyrao on 14-3-20.
 */
public class VideoAppToTupleTest extends TestCase
{
    @Test
    public void testExtractLastField() throws Exception
    {
        String test = "(2014-03-20 16:14:07,10.187.136.157,,,,,2086,0,1100214736,{\"ui\":\"863343021326715\"\\,\"ky\":\"AES1V77ZTU79\"\\,\"idx\":2430\\,\"ts\":1395303254\\,\"mc\":\"30:9b:ad:96:fd:01\"\\,\"kv\":{\"cgi_requesturl\":\"http://mobile.video.qq.com/fcgi-bin/getjimudata?ztid=100109&idxs=145%2B146%2B147%2B148%2B149%2B150%2B151%2B152%2B153%2B154&keytplid=33%2B56%2B55%2B31%2B29&type=1&otype=json&version=20500&qqlog=&appver=3.1.0.5826&market_id=285&install_time=1394868170&platform=8&sysver=4.2.2&device=vivo+Y11i+T&lang=zh\"\\,\"cgi_retrytimes\":\"1\"\\,\"cgi_connection_duration\":\"42\"\\,\"cgi_readdata_duration\":\"81\"\\,\"cgi_moduleid\":\"1101\"\\,\"cgi_requestip\":\"113.108.16.120\"}\\,\"ei\":\"boss_cgi_duration\"\\,\"cui\":\"86a7e828acfc11e385efabcd0f0abb0a\"\\,\"ch\":\"285\"\\,\"ut\":1\\,\"et\":1000\\,\"av\":\"3.1.0.5826\"\\,\"si\":1529181009\\,\"ip\":\"36.22.27.173\"\\,\"id\":1100214736})";

        assertEquals("{\"ui\":\"863343021326715\",\"ky\":\"AES1V77ZTU79\",\"idx\":2430,\"ts\":1395303254,\"mc\":\"30:9b:ad:96:fd:01\",\"kv\":{\"cgi_requesturl\":\"http://mobile.video.qq.com/fcgi-bin/getjimudata?ztid=100109&idxs=145%2B146%2B147%2B148%2B149%2B150%2B151%2B152%2B153%2B154&keytplid=33%2B56%2B55%2B31%2B29&type=1&otype=json&version=20500&qqlog=&appver=3.1.0.5826&market_id=285&install_time=1394868170&platform=8&sysver=4.2.2&device=vivo+Y11i+T&lang=zh\",\"cgi_retrytimes\":\"1\",\"cgi_connection_duration\":\"42\",\"cgi_readdata_duration\":\"81\",\"cgi_moduleid\":\"1101\",\"cgi_requestip\":\"113.108.16.120\"},\"ei\":\"boss_cgi_duration\",\"cui\":\"86a7e828acfc11e385efabcd0f0abb0a\",\"ch\":\"285\",\"ut\":1,\"et\":1000,\"av\":\"3.1.0.5826\",\"si\":1529181009,\"ip\":\"36.22.27.173\",\"id\":1100214736}", VideoAppToTuple.extractLastField(test).get());

    }


    @Test
    public void testToList() throws Exception
    {
//        String test = "(2014-03-20 16:14:07,10.187.136.157,,,,,2086,0,1100214736,{\"ui\":\"863343021326715\"\\,\"ky\":\"AES1V77ZTU79\"\\,\"idx\":2430\\,\"ts\":1395303254\\,\"mc\":\"30:9b:ad:96:fd:01\"\\,\"kv\":{\"cgi_requesturl\":\"http://mobile.video.qq.com/fcgi-bin/getjimudata?ztid=100109&idxs=145%2B146%2B147%2B148%2B149%2B150%2B151%2B152%2B153%2B154&keytplid=33%2B56%2B55%2B31%2B29&type=1&otype=json&version=20500&qqlog=&appver=3.1.0.5826&market_id=285&install_time=1394868170&platform=8&sysver=4.2.2&device=vivo+Y11i+T&lang=zh\"\\,\"cgi_retrytimes\":\"1\"\\,\"cgi_connection_duration\":\"42\"\\,\"cgi_readdata_duration\":\"81\"\\,\"cgi_moduleid\":\"1101\"\\,\"cgi_requestip\":\"113.108.16.120\"}\\,\"ei\":\"boss_cgi_duration\"\\,\"cui\":\"86a7e828acfc11e385efabcd0f0abb0a\"\\,\"ch\":\"285\"\\,\"ut\":1\\,\"et\":1000\\,\"av\":\"3.1.0.5826\"\\,\"si\":1529181009\\,\"ip\":\"36.22.27.173\"\\,\"id\":1100214736})";
        String test = "2014-03-17 23:59:53,10.209.21.90,,,,,2086,0,1100214736,{\"ui\":\"867295001249174\"\\,\"ky\":\"AES1V77ZTU79\"\\,\"idx\":14470\\,\"ts\":1395071993\\,\"mc\":\"68:9c:5e:82:94:56\"\\,\"kv\":{\"vod_live_dlType\":\"1\"\\,\"vod_live_playerId\":\"b0013aiu5tj\"\\,\"cmd\":\"13\"\\,\"vod_live_playMode\":\"1\"\\,\"vod_live_curentPlayCoverID\":\"jhw2ygv4tkdmsdl\"\\,\"vod_live_adReportStatus\":\"0\"\\,\"vod_live_playUrl\":\"http://124.14.15.20/vlive.qqvideo.tc.qq.com/\"\\,\"vod_live_firstFrameOrUrl\":\"12\"\\,\"vod_live_fetchPlayUrlStatusCode\":\"2\"\\,\"vod_live_progType\":\"1036\"\\,\"vod_live_cdnId\":\"203\"\\,\"vod_live_ReportTypePlayStatus\":\"1\"}\\,\"ei\":\"boss_cmd_vv\"\\,\"cui\":\"e3364051a38611e385efabcd0496bb0a\"\\,\"ch\":\"250\"\\,\"ut\":1\\,\"et\":1000\\,\"av\":\"3.1.0.5826\"\\,\"si\":145251608\\,\"ip\":\"218.11.178.124\"\\,\"id\":1100214736}";
        List<Object> result = VideoAppToTuple.toList(test);
        System.out.println(Arrays.toString(result.toArray()));


        /*
          result.add(ui);
        result.add(vod_live_cdnId);
        result.add(vod_live_playerId);
        result.add(ei);
        result.add(et);
        result.add(id);
         */
    }
}
