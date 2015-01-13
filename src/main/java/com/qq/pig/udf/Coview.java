package com.qq.pig.udf;

import com.google.common.collect.Sets;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.Set;

/**
 * Created by antyrao on 14-7-9.
 */
public class Coview extends EvalFunc<DataBag>
{

    private String movieid;

    public Coview(String movieid)
    {
        this.movieid = movieid;
    }

    DataBag empty = BagFactory.getInstance().newDefaultBag();

    public DataBag exec(Tuple input) throws IOException
    {
        if (input == null || input.size() == 0)
        {
            return null;
        }
        try
        {
            DataBag bag = (DataBag) input.get(0);
            if (bag == null)
                return null;

            Set<String> moviesWatched = Sets.newHashSet();
            for (Tuple tuple : bag)
            {
                String movie = (String) tuple.get(0);
                moviesWatched.add(movie);
            }
            if (moviesWatched.contains(movieid))
            {
                moviesWatched.remove(movieid);
                DataBag result = BagFactory.getInstance().newDefaultBag();
                for (String m : moviesWatched)
                {
                    Tuple tuple = TupleFactory.getInstance().newTuple(1);
                    tuple.set(0, m);
                    result.add(tuple);
                }
                return result;
            }
            return null;
        }
        catch (NumberFormatException e)
        {
            return null;
        }
        catch (Exception ee)
        {
            throw new RuntimeException("Error while creating a bag", ee);
        }
    }
}