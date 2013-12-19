package com.qq.pig.udf;

import com.google.common.base.Splitter;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: antyrao
 * Date: 13-11-12
 * Time: 下午4:49
 */
public class ExtractChannelBag extends EvalFunc<DataBag>
{

    private Splitter commaSplitter = Splitter.on(",");

    public DataBag exec(Tuple input) throws IOException
    {
        if (input == null || input.size() == 0)
        {
            return null;
        }
        try
        {

            DataBag output = DefaultBagFactory.getInstance().newDefaultBag();
            String channelInterestStr = (String) input.get(0);
            Iterable<String> iterable = commaSplitter.split(channelInterestStr);
            for (String channelInterest : iterable)
            {
                int index = endIndexOfChannel(channelInterest);
                if (index < 0)
                {
                    continue;
                }

                String channel = channelInterest.substring(0, index);
                String weight = channelInterest.substring(index + 1);
                if (channel.length() == 0)
                    continue;

                Tuple t = TupleFactory.getInstance().newTuple(2);
                t.set(0, channel);
                t.set(1, weight);
                output.add(t);

            }
            return output;
        }
        catch (Exception e)
        {
            System.err.println("ExtractChannelBag: failed to process input; error - " + e.getMessage());
            return null;
        }
    }

//    private void populateWithZero(Tuple tuple) throws ExecException
//    {
//        for (int i = 0; i < channelCount; i++)
//        {
//            tuple.set(i, 0);
//        }
//    }


    /**
     * if passed in <code>channelColumnInterest</code> contains interest of channel,
     * return index of semicolon, otherwise return -1
     *
     * @param channelColumnInterest
     * @return
     */
    public static int endIndexOfChannel(String channelColumnInterest)
    {
        int firstIndex = channelColumnInterest.indexOf(":");
        int secondIndex = channelColumnInterest.indexOf(":", firstIndex + 1);
        //channel interest contains only one ":"
        //news:newsgn:gdxw:0.4541,news:0.2726,news:newsgn:0.2762,zj:news:0.7541,zj:0.1662 20131209
        if (secondIndex < 0)
        {
            return firstIndex;
        }
        return -1;
    }

//    @Override

    /**
     * This method gives a name to the column.
     *
     * @param input - schema of the input data
     * @return schema of the input data
     */
    public Schema outputSchema(Schema input)
    {
        Schema bagSchema = new Schema();
        bagSchema.add(new Schema.FieldSchema("channel", DataType.CHARARRAY));
        bagSchema.add(new Schema.FieldSchema("weight", DataType.CHARARRAY));
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
//    @Override
//    public List<FuncSpec> getArgToFuncMapping() throws FrontendException
//    {
//        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
//        funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));
//
//        return funcList;
//    }
}
