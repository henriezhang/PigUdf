package com.qq.pig.udf;

import com.google.common.base.Throwables;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: antyrao
 * Date: 13-11-25
 * Time: 下午5:39
 */
public class PriceAvg extends EvalFunc<Integer>
{

    private PriceLevel leveler = new PriceLevel();

    public Integer exec(Tuple input) throws IOException
    {
        if (input == null || input.size() == 0)
            return null;
        try
        {
            String strPrice = (String) input.get(0);
            int indexDash = strPrice.indexOf("-");
            if (indexDash < 0)
            {
                return null;
            }
            double low = Double.parseDouble(strPrice.substring(0, indexDash));
            double high = Double.parseDouble(strPrice.substring(indexDash + 1));
            double avg = (low + high) / 2;
            //only keep two decimal place
            avg = (double) Math.round(avg * 100) / 100;

            return leveler.getLevel(avg);
        }
        catch (Exception e)
        {
            System.err.println("Price Avg: failed to process input; error - " + Throwables.getStackTraceAsString(e));
            return null;
        }
    }

    @Override
    /**
     * This method gives a name to the column.
     * @param input - schema of the input data
     * @return schema of the input data
     */
    public Schema outputSchema(Schema input)
    {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.INTEGER));
    }

    /* (non-Javadoc)
     * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
     * This is needed to make sure that both bytearrays and chararrays can be passed as arguments
     */
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException
    {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));

        return funcList;
    }

}

