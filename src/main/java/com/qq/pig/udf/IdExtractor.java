package com.qq.pig.udf;

import com.google.common.collect.Lists;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: antyrao
 * Date: 13-11-12
 * Time: 下午4:51
 */
public class IdExtractor
{
    public static final String BRAND_ID = "brand_id";
    public static final String CAR_SERIAL = "car_serial";
    public static final String SERIAL = "serial";
    public static final String CAR_MODELS = "car_models";
    public static final String SID = "sid";
    public static final String CAR_EVALUAT = "car_evaluat";
    public static final String CAR_PURCHASES = "car_purchases";
    public static final String CAR_VIDEO = "car_video";
    public static final String CMPSTR = "cmpstr";

    //keywords to extract car serial id only
    private static final List<String> PATH_KEYS;
    private static final List<String> QUERY_KEYS;
    private static final List<String> FRAGMENT_KEYS;

    static
    {
        PATH_KEYS = Lists.newArrayList();
        PATH_KEYS.add(CAR_SERIAL);
        PATH_KEYS.add(CAR_EVALUAT);
        PATH_KEYS.add(CAR_PURCHASES);
        PATH_KEYS.add(CAR_VIDEO);

        QUERY_KEYS = Lists.newArrayList();
        QUERY_KEYS.add(SERIAL);

        FRAGMENT_KEYS = Lists.newArrayList();
        FRAGMENT_KEYS.add(SID);

    }


    public static final Charset UTF8 = Charset.forName("utf-8");


    private String extractFromPath(String url)
    {
        String result;
        for (String path : PATH_KEYS)
        {
            result = extractFromPath(url, path);
            if (result != null)
            {
                return result;
            }
        }
        return null;
    }

    private String extractFromPath(String url, String lookFor)
    {
        int index = url.indexOf(lookFor);
        if (index < 0)
        {
            return null;
        }
        int start = index + lookFor.length() + 1;
        int end = url.indexOf("/", start);
        if (end < 0)
        {
            return null;
        }

        return url.substring(start, end);

    }


    private String extractFromQueryString(String url)
    {
        int indexOfQuestionMark = url.indexOf("?");
        if (indexOfQuestionMark < 0)
        {
            return null;
        }
        url = url.substring(indexOfQuestionMark + 1);
        String result;
        for (String key : QUERY_KEYS)
        {
            result = extractFromQueryString(url, key);
            if (result != null)
            {
                return result;
            }
        }
        return null;
    }

    private String extractFromQueryString(String url, String name)
    {
        List<NameValuePair> paras = URLEncodedUtils.parse(url, UTF8);
        for (NameValuePair pair : paras)
        {
            if (pair.getName().equals(name))
            {
                return pair.getValue();
            }
        }
        return null;
    }

    private String extractFromFragmentString(String url)
    {
        int hashIndex = url.indexOf("#");

        if (hashIndex < 0)
        {
            return null;
        }
        url = url.substring(hashIndex + 1);
        String result;
        for (String key : FRAGMENT_KEYS)
        {
            result = extractFromFragmentString(url, key);
            if (result != null)
            {
                return result;
            }
        }
        return null;
    }

    private String extractFromFragmentString(String url, String name)
    {
        List<NameValuePair> paras = URLEncodedUtils.parse(url, UTF8);
        for (NameValuePair pair : paras)
        {
            if (pair.getName().equals(name))
            {
                return pair.getValue();
            }
        }
        return null;
    }

    private void extractCarSerialId()
    {

    }

    private String extractBrandId(String url)
    {
        return extractFromQueryString(url, BRAND_ID);
    }

    private List<String> extractCarTypeId(String url)
    {

        List<String> result = Lists.newArrayList();

        String id = extractFromPath(url, CAR_MODELS);
        if (id != null)
        {
            result.add(id);
            return result;
        }

        id = extractFromFragmentString(url, CMPSTR);

        if (id.indexOf(",") > 0)
        {
            String[] ids = id.split(",");
            for (String item : ids)
            {
                if (!item.equals("0"))
                {
                    result.add(item);
                }
            }
        }
        else
        {
            result.add(id);
        }
        return result;
    }


    public List<String> extractId(String url)
    {
        List<String> result = Lists.newArrayList();

        String id = extractFromPath(url);
        if (id != null)
        {
            result.add(id);
            return result;
        }
        id = extractFromQueryString(url);
        if (id != null)
        {
            result.add(id);
            return result;
        }
        id = extractFromFragmentString(url);
        if (id != null)
        {
            if (id.indexOf(",") > 0)
            {
                String[] ids = id.split(",");
                for (String item : ids)
                {
                    if (!item.equals("0"))
                    {
                        result.add(item);
                    }
                }
            }
            else
            {
                result.add(id);
            }
            return result;
        }
        return result;
    }


    public static class Pair<K, V>
    {
        private K left;
        private V right;


        public Pair(K left, V right)
        {
            this.left = left;
            this.right = right;
        }

        public K getLeft()
        {
            return left;
        }

        public V getRight()
        {
            return right;
        }
    }


}
