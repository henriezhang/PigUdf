package com.qq.pig.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * Created by antyrao on 14-8-15.
 */
public class VecAvgOptimize extends EvalFunc<String>
{
    @Override
    public String exec(Tuple input) throws IOException
    {
        if (input == null || input.size() == 0)
            return null;
        try
        {
            DataBag dataBag = (DataBag) input.get(0);
            if (dataBag == null || dataBag.size() == 0)
                return null;
            double[] result = null;
            int count = 0;
            for (Tuple tuple : dataBag)
            {
//                String strVec = (String) tuple.get(0);
                double[] vector = fromTuple(tuple); //toDouble(strVec);
                if (result == null)
                    result = new double[vector.length];
                result = sum(result, vector);
                count++;
            }
            result = divide(result, count);
            return toStr(result);
        }
        catch (Exception e)
        {
            System.err.println("Price Avg: failed to process input; error - ");
            return null;
        }
    }


    private static double[] fromTuple(Tuple tuple) throws ExecException
    {
        double[] result = new double[tuple.size() - 1];
        for (int i = 1; i < tuple.size(); i++)
        {
            result[i - 1] = (Double) tuple.get(i);
        }
        return result;
    }

    private static double[] sum(double[] a, double[] b)
    {
        assert a.length == b.length;
        for (int i = 0; i < a.length; i++)
        {
            a[i] = a[i] + b[i];
        }
        return a;
    }

    private static double[] toDouble(String strVec)
    {
        String[] temps = strVec.split(" ");
        double[] vector = new double[temps.length];
        for (int i = 0; i < vector.length; i++)
        {
            vector[i] = Double.valueOf(temps[i]);
        }
        return vector;
    }

    private static String toStr(double[] a)
    {
        StringBuilder sb = new StringBuilder();
        for (double item : a)
        {
            sb.append(item);
            sb.append(" ");
        }
        if (sb.length() > 0)
        {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    private static double[] divide(double[] a, int divisor)
    {
        for (int i = 0; i < a.length; i++)
        {
            a[i] = a[i] / divisor;
        }
        return a;
    }
}
