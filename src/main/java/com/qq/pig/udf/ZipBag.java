package com.qq.pig.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by antyrao on 14-5-15.
 */
public class ZipBag extends EvalFunc<DataBag>
{

    @Override
    public DataBag exec(Tuple input) throws IOException
    {
        if (input == null || input.size() == 0)
        {
            return null;
        }
        try
        {
            DataBag bag1 = (DataBag) input.get(0);
            DataBag bag2 = (DataBag) input.get(1);
            if (bag1 == null || bag2 == null)
            {
                return null;
            }
            if (bag1.size() != bag2.size())
                return null;
            // The assumption is that if the bag contents fits into
            // an input tuple, it will not need to be spilled.
            DataBag bag = new NonSpillableDataBag((int) bag1.size());

            Iterator<Tuple> iterator1 = bag1.iterator();
            Iterator<Tuple> iterator2 = bag2.iterator();
            while (iterator1.hasNext())
            {
                Tuple tuple1 = iterator1.next();
                Tuple tuple2 = iterator2.next();
                Tuple tuple = TupleFactory.getInstance().newTuple(2);
                tuple.set(0, tuple1.get(0));
                tuple.set(1, tuple2.get(0));

                bag.add(tuple);
            }

            return bag;
        }
        catch (Exception ee)
        {
            throw new RuntimeException("Error while creating a bag", ee);
        }
    }

}
