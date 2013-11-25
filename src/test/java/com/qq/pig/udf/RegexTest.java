package com.qq.pig.udf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: antyrao
 * Date: 13-11-13
 * Time: 下午7:56
 */
public class RegexTest
{
    public static void main(String[] args)
    {
        Pattern urlPattern = Pattern.compile("^\\d{5,}$");

        Matcher matcher = urlPattern.matcher("100111111112222");

        System.out.println(matcher.matches());
    }
}
