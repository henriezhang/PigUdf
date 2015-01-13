package com.qq.pig.udf;

//import com.google.common.base.Throwables;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * Created by antyrao on 14-2-11.
 */
public class NormalizePgv extends EvalFunc<String>
{

    private String normalize(String line)
    {
//        Splitter splitter = Splitter.on(",");
//        List<String> items = splitter.splitToList(line);
        String[] items = line.split(",");
        if (items.length != 19)
            return null;

        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < items.length; i++)
        {
            sb.append(items[i]);
            sb.append("|");
        }
        if (sb.length() > 0)
        {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    @Override
    public String exec(Tuple input) throws IOException
    {
        if (input == null || input.size() == 0)
            return null;
        try
        {
            String line = (String) input.get(0);
            return normalize(line);
        }
        catch (Exception e)
        {
            System.err.println("NormalizePgv failed with error message  ");
            return null;
        }
    }
}
