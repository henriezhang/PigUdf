package com.qq.pig.udf;

//import com.google.common.base.Splitter;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;

/**
 * Created by antyrao on 14-3-25.
 */
public class SplitStrToTuple extends EvalFunc<Tuple>
{


    private String delimiter = ",";
//    Splitter splitter;

    public SplitStrToTuple()
    {
//        splitter = Splitter.on(delimiter);
    }

    public SplitStrToTuple(String delimiter)
    {
        this.delimiter = delimiter;
//        splitter = Splitter.on(delimiter);
    }

    @Override

    public Tuple exec(Tuple input) throws IOException
    {
        if (input == null || input.size() == 0)
        {
            return null;
        }
        try
        {
            String str = (String) input.get(0);
            if (str == null)
                return null;
            String[] strList = str.split(delimiter);

            Tuple tuple = TupleFactory.getInstance().newTuple(strList.length);

            for (int i = 0; i < strList.length; ++i)
            {
                tuple.set(i, strList[i]);
            }

            return tuple;
        }
        catch (Exception ee)
        {
            throw new RuntimeException("Error while creating a tuple", ee);
        }
    }

    public static void main(String[] args)
    {
        String[] items = ":0.1278".split(":");
        System.out.println(items[0]);
        System.out.println(items[1]);

    }
}
