package com.qq.pig.udf;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: antyrao
 * Date: 13-12-18
 * Time: 下午6:33
 */
public class ColumnParser
{

    private static final List<String> EMPTY = Lists.newArrayList();


    private Splitter splitter = Splitter.on(".");


    public List<String> parseColumns(String col)
    {


        int index = col.indexOf("L.");
        if (index < 0)
        {
            return EMPTY;
        }
        int endPos = col.indexOf("_", index + 2);
        if (endPos < 0)
        {
            return EMPTY;
        }
        String colStr = col.substring(index + 2, endPos);
        return splitter.splitToList(colStr);
    }

    public static void main(String[] args)
    {
        ColumnParser parser = new ColumnParser();
        String str = "L.sports.basket.bbhotnews_Z._W.2_M.3_P.sports";
        for (String item : parser.parseColumns(str))
        {
            System.out.println(item);
        }
    }

}
