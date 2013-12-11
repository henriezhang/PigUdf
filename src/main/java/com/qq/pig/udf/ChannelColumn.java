package com.qq.pig.udf;

import com.google.common.base.Splitter;
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
 * Date: 13-11-12
 * Time: 下午4:49
 */
public class ChannelColumn extends EvalFunc<DataBag>
{

    public DataBag exec(Tuple input) throws IOException
    {
        if (input == null || input.size() == 0)
        {
            return null;
        }
        try
        {
            DataBag output = DefaultBagFactory.getInstance().newDefaultBag();
            String url = (String) input.get(0);

            Iterable<String> interests = Splitter.on(",").split(url);
            for (String interest : interests)
            {
                int index = interest.lastIndexOf(":");
                if (index < 0)
                {
                    continue;
                }
                Tuple t = TupleFactory.getInstance().newTuple(1);

                t.set(0, interest.substring(0, index));

                output.add(t);
            }

            return output;
        }
        catch (Exception e)
        {
            System.err.println("NGramGenerator: failed to process input; error - " + e.getMessage());
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
        Schema bagSchema = new Schema();
        bagSchema.add(new Schema.FieldSchema("column", DataType.CHARARRAY));
        try
        {
            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                    bagSchema, DataType.BAG));
        }
        catch (FrontendException e)
        {
            return null;
        }
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
