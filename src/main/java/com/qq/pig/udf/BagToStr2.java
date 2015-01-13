package com.qq.pig.udf;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.List;

/**
 * Created by antyrao on 14-4-14.
 */
public class BagToStr2 extends EvalFunc<String>
{
    public static final String DEFAULT_TUPLE_SEPARATOR = ";";
    public static final String DEFAULT_FIELD_SEPARATOR = ",";
    private String tupleSeparator;
    private String fieldSeparator;

    public BagToStr2()
    {
        tupleSeparator = DEFAULT_TUPLE_SEPARATOR;
        fieldSeparator = DEFAULT_FIELD_SEPARATOR;
    }

    public BagToStr2(String separator)
    {
        if (separator.length() > 0)
            tupleSeparator = separator.substring(0, 1);
        if (separator.length() > 1)
            fieldSeparator = separator.substring(1, 2);
    }


    @Override
    public String exec(Tuple input) throws IOException
    {
        if (input == null || input.size() == 0)
        {
            return null;
        }
        try
        {
            DataBag dataBag = (DataBag) input.get(0);
            if (dataBag == null || dataBag.size() == 0)
                return null;
            StringBuilder sb = new StringBuilder();
            for (Tuple tuple : dataBag)
            {
                if (tuple.size() < 2)
                    continue;
                List<Object> all = tuple.getAll();
                //remove first
                all.remove(0);
                String str = Joiner.on(":").useForNull("").join(all);
                sb.append(str);
                sb.append(";");
            }
            if (sb.length() > 1)
            {
                //remove trailing <i>;<i>
                sb.setLength(sb.length() - 1);
            }
            return sb.toString();
        }
        catch (Exception e)
        {
            System.err.println("BagToStr: failed to process input; error - " + e.getMessage() + "," + Throwables.getStackTraceAsString(e));
            return null;
        }

    }

}
