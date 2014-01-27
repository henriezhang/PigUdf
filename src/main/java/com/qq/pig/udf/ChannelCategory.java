package com.qq.pig.udf;

import com.google.common.base.Splitter;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: antyrao
 * Date: 13-12-25
 * Time: 上午11:11
 */
public class ChannelCategory extends EvalFunc<Integer>
{

    @Override
    public Integer exec(Tuple input) throws IOException
    {
        if (input == null || input.size() == 0)
        {
            return null;
        }
        try
        {
//            DataBag output = DefaultBagFactory.getInstance().newDefaultBag();
            Integer weight = (Integer) input.get(0);

            if (weight >= 0 && weight < 10)
            {
                return 1;
            }

            if (weight > 10 && weight < 20)
            {
                return 2;
            }


//            Iterable<String> interests = Splitter.on(",").split(url);
//
//            for (String interest : interests)
//            {
//                int index = interest.lastIndexOf(":");
//                if (index < 0)
//                {
//                    continue;
//                }
//                Tuple t = TupleFactory.getInstance().newTuple(1);
//
//                t.set(0, interest.substring(0, index));
//
//                output.add(t);
//            }
//
//            return output;
        }
        catch (Exception e)
        {
            System.err.println("ChannelCategory: failed to process input; error - " + e.getMessage());
            return null;
        }

        return 1;
    }
}
