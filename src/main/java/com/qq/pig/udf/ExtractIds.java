package com.qq.pig.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: antyrao
 * Date: 13-11-12
 * Time: 下午4:49
 */
public class ExtractIds extends EvalFunc<DataBag>
{

    private static IdExtractor extractor = new IdExtractor();

    private static Pattern digitPattern = Pattern.compile("^\\d+$");

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
            List<List<String>> ids = extractor.extractId(url);
            for (List<String> id : ids)
            {
                Tuple t = TupleFactory.getInstance().newTuple(3);
                int index = 0;
                for (String type : id)
                {
                    if (type == null)
                    {
                        t.set(index, null);
                    }
                    else
                    {
                        Matcher matcher = digitPattern.matcher(type);
                        if (!matcher.matches())
                        {
                            t.set(index, null);
                        }
                        else
                        {
                            t.set(index, type);
                        }
                    }

                    index++;
                }
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
        bagSchema.add(new Schema.FieldSchema("brandid", DataType.CHARARRAY));
        bagSchema.add(new Schema.FieldSchema("carserialid", DataType.CHARARRAY));
        bagSchema.add(new Schema.FieldSchema("cartypeid", DataType.CHARARRAY));
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
