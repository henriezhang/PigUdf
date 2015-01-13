package com.qq.pig.udf;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: antyrao
 * Date: 13-12-18
 * Time: 下午5:29
 */
public class SportColumnDict
{

    private static final Multimap<String, String> columnDict = ArrayListMultimap.create();

    static
    {
        columnDict.put("isocce", "footable");//足球
        columnDict.put("csocce", "footable");

        columnDict.put("isocce", "international_footable");//国际足球
        columnDict.put("csocce", "chinese_footable");//中国足球

        columnDict.put("basket", "basket");//篮球
        columnDict.put("cba", "basket");

        columnDict.put("cba", "chinese_basket");//中国篮球

        columnDict.put("nba", "nba");     //nba
        columnDict.put("bbhotnews", "nba");
        columnDict.put("vnba", "nba");
        columnDict.put("nbaas", "nba");
        columnDict.put("nbapics", "nba");
        columnDict.put("nbachinagamespic", "nba");
        columnDict.put("nbamiracle", "nba");
        columnDict.put("30skills", "nba");
        columnDict.put("hsnba", "nba");
        columnDict.put("topgame", "nba");


        columnDict.put("lottery", "lottery");//lottery

        columnDict.put("chess", "chess");//chess
        columnDict.put("weiqi", "chess");//chess
        columnDict.put("qppic", "chess");//chess

        columnDict.put("saiche", "Racing");//赛车
        columnDict.put("f1", "Racing");

        columnDict.put("wangqiu", "tennis");//tennis
        columnDict.put("tennis", "tennis");//tennis
        columnDict.put("tphoto", "tennis");//tennis

        columnDict.put("xijia", "xijia");//xijia


        columnDict.put("yingc", "yingc");//yingc
        columnDict.put("dejia", "dejia");//dejia
        columnDict.put("yijia", "yijia");//yijia
        columnDict.put("chamleg", "chamleg");//chamleg
        columnDict.put("jiaa", "jiaa");


        columnDict.put("ppq", "ppq");
        columnDict.put("ppqpic", "ppq");
        columnDict.put("otherscomping", "ppq");

        columnDict.put("ticao", "gym");
        columnDict.put("tcpic", "gym");

        columnDict.put("swimin", "swimming");
        columnDict.put("dyypic", "swimming");
        columnDict.put("otherscomyouyong", "swimming");


        columnDict.put("tableb", "billiards");
        columnDict.put("tqpic", "billiards");
        columnDict.put("otherscomtaiqiu", "billiards");

        columnDict.put("kongfuboxing", "kongfuboxing");
        columnDict.put("kungfupic", "kongfuboxing");
        columnDict.put("mma", "kongfuboxing");


        columnDict.put("otherszjj", "otherszjj");

        columnDict.put("paiq", "volleyball");
        columnDict.put("pqpic", "volleyball");
        columnDict.put("paiqiupl", "volleyball");


        columnDict.put("ymq", "badminton");
        columnDict.put("ymqpic", "badminton");

        columnDict.put("dongji", "fargo");
        columnDict.put("bxpic", "fargo");

        columnDict.put("tianj", "athletics");
        columnDict.put("tjpic", "athletics");

        columnDict.put("electronic", "electronic");

        columnDict.put("gaoerfu", "gaoerfu");
        columnDict.put("gefpic", "gaoerfu");

        columnDict.put("equestrian", "equestrian");
        columnDict.put("run", "run");
    }

    private static final List<String> columns = new ArrayList<String>();
    private static final Map<String, Integer> columnPositionMap = new HashMap<String, Integer>();

    static
    {
        columns.add("footable");
        columns.add("international_footable");
        columns.add("chinese_footable");
        columns.add("basket");
        columns.add("chinese_basket");
        columns.add("nba");
        columns.add("lottery");
        columns.add("chess");
        columns.add("racing");
        columns.add("tennis");
        columns.add("xijia");
        columns.add("yingc");
        columns.add("dejia");
        columns.add("yijia");
        columns.add("chamleg");
        columns.add("jiaa");
        columns.add("ppq");
        columns.add("gym");
        columns.add("swimming");
        columns.add("billiards");
        columns.add("kongfuboxing");
        columns.add("otherszjj");
        columns.add("volleyball");
        columns.add("badminton");
        columns.add("fargo");
        columns.add("athletics");
        columns.add("electronic");
        columns.add("gaoerfu");
        columns.add("equestrian");
        columns.add("run");

        int index = 0;
        for (String column : columns)
        {
            columnPositionMap.put(column, index++);
        }

    }


    public Collection<String> getMappedColumn(String column)
    {
        return columnDict.get(column);
    }

    public List<String> getColumns()
    {
        return columns;
    }

    public Integer getColumnPos(String column)
    {
        return columnPositionMap.get(column);
    }

    public static void main(String[] args)
    {
        Collection<String> v = columnDict.get("isocce");
        System.out.println(v);
    }
}





















