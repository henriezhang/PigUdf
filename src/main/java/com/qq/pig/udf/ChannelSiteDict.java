package com.qq.pig.udf;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: antyrao
 * Date: 13-12-11
 * Time: 下午6:54
 */
public class ChannelSiteDict
{
    private static final List<String> channels = Lists.newArrayList();

    static
    {
        channels.add("adver");
        channels.add("ah");
        channels.add("app");
        channels.add("astro");
        channels.add("auto");
        channels.add("baby");
        channels.add("bb");
        channels.add("bbs");
        channels.add("bbs1");
        channels.add("bella");
        channels.add("bellazt");
        channels.add("boaoforum");
        channels.add("book");
        channels.add("cd");
        channels.add("comic");
        channels.add("comment1");
        channels.add("cq");
        channels.add("cqsc");
        channels.add("cqwebapp");
        channels.add("cul");
        channels.add("cva");
        channels.add("digi");
        channels.add("discovery");
        channels.add("download");
        channels.add("dy");
        channels.add("dyaio");
        channels.add("edu");
        channels.add("ent");
        channels.add("expo");
        channels.add("fashion");
        channels.add("fiba");
        channels.add("finance");
        channels.add("fj");
        channels.add("flash");
        channels.add("fm");
        channels.add("gamezone");
        channels.add("gamezonealbum");
        channels.add("gd");
        channels.add("gonglue");
        channels.add("gongyi");
        channels.add("gter");
        channels.add("hb");
        channels.add("health");
        channels.add("henan");
        channels.add("hn");
        channels.add("home3g");
        channels.add("house");
        channels.add("housedongguan");
        channels.add("househangzhou");
        channels.add("houseqingyuan");
        channels.add("houseshenyang");
        channels.add("house_changsha");
        channels.add("house_chengdu");
        channels.add("house_chongqing");
        channels.add("house_foshan");
        channels.add("house_fuzhou");
        channels.add("house_guangzhou");
        channels.add("house_huizhou");
        channels.add("house_shanghai");
        channels.add("house_shenzhen");
        channels.add("house_wuhan");
        channels.add("house_xiamen");
        channels.add("house_xian");
        channels.add("house_yichang");
        channels.add("house_zhengzhou");
        channels.add("house_zhuhai");
        channels.add("i4s");
        channels.add("img1");
        channels.add("imgwap");
        channels.add("imqq");
        channels.add("ipad");
        channels.add("jiangsu");
        channels.add("joke");
        channels.add("joy");
        channels.add("juventus");
        channels.add("kid");
        channels.add("lady");
        channels.add("lequipe");
        channels.add("ln");
        channels.add("luxury");
        channels.add("magazine");
        channels.add("mapp");
        channels.add("mat1");
        channels.add("mb");
        channels.add("media");
        channels.add("microblog");
        channels.add("mini");
        channels.add("mini2009");
        channels.add("mini2012");
        channels.add("minisite");
        channels.add("minixsoso");
        channels.add("mobile");
        channels.add("music");
        channels.add("musictop");
        channels.add("musictop2");
        channels.add("nanjing");
        channels.add("nanjing2014");
        channels.add("neikan");
        channels.add("news");
        channels.add("olympic");
        channels.add("paipai");
        channels.add("paipai3g");
        channels.add("paipaibbs");
        channels.add("paipaidigi");
        channels.add("paipaigames");
        channels.add("paipailady");
        channels.add("paipailife");
        channels.add("paipaimai");
        channels.add("paipaiman");
        channels.add("panel");
        channels.add("pcdesk");
        channels.add("pingjs");
        channels.add("pinglun");
        channels.add("qbar");
        channels.add("qqusportnews");
        channels.add("quan");
        channels.add("realmadrid");
        channels.add("rushidao");
        channels.add("sd");
        channels.add("search");
        channels.add("service");
        channels.add("sh");
        channels.add("sing");
        channels.add("sms");
        channels.add("sports");
        channels.add("stock");
        channels.add("survey");
        channels.add("tad");
        channels.add("tech");
        channels.add("tencentmind");
        channels.add("tv");
        channels.add("uppower");
        channels.add("v");
        channels.add("view");
        channels.add("wbzt");
        channels.add("weather");
        channels.add("webchat");
        channels.add("wizard");
        channels.add("worldcup");
        channels.add("www");
        channels.add("xian");
        channels.add("xmfj");
        channels.add("xsoso");
        channels.add("xxx");
        channels.add("yslp");
        channels.add("zhengzhou");
        channels.add("zj");
        channels.add("zonetenpay");
        channels.add("weishi");
        channels.add("housenanjing");
    }

    private Map<String, Integer> channelPos = Maps.newHashMap();


    public ChannelSiteDict()
    {
        int i = 0;
        for (String channel : channels)
        {
            channelPos.put(channel, i++);
        }
    }

    public int channelCount()
    {
        return channels.size();
    }

    public Integer getPos(String channel)
    {
        return channelPos.get(channel);
    }

}
