package com.qq.pig.udf;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: antyrao
 * Date: 13-11-12
 * Time: 下午4:49
 */
public class FlatternColumn extends EvalFunc<Tuple>
{
    private SportColumnDict dict = new SportColumnDict();
    private int count = dict.getColumns().size();

    public Tuple exec(Tuple input) throws IOException
    {
        if (input == null || input.size() == 0)
            return null;

        try
        {
            Tuple output = TupleFactory.getInstance().newTuple(count);
            DataBag dataBag = (DataBag) input.get(0);
            if (dataBag == null)
            {
                return null;
            }
            Utils.populateWithZero(output);

            Iterator<Tuple> iterator = dataBag.iterator();
            while (iterator.hasNext())
            {
                Tuple tuple = iterator.next();
                if (tuple.size() < 3)
                    continue;
                String columnName = (String) tuple.get(1);
                Double interest = (Double) tuple.get(2);
                if (Strings.isNullOrEmpty(columnName) || interest == null)
                    continue;
                Integer position = dict.getColumnPos(columnName);
                if (position == null)
                {
                    throw new RuntimeException(columnName + " doesn't exist in dictionary");
                }
                if (interest == null)
                {
                    throw new RuntimeException(columnName + " interest is null");
                }
                int normalizeInterest = Utils.roundUpToInt(interest.floatValue());
                output.set(position, normalizeInterest);
            }
            return output;
        }
        catch (Exception e)
        {
            System.err.println("ExtractChannelBag: failed to process input; error - " + e.getMessage() + "," + Throwables.getStackTraceAsString(e));
            return null;
        }
    }


//    private int scaleToInt(double interest)
//    {
//        return (int) Math.round(interest * 100);
//    }

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
//    public static int endIndexOfChannel(String channelColumnInterest)
//    {
//        int firstIndex = channelColumnInterest.indexOf(":");
//        int secondIndex = channelColumnInterest.indexOf(":", firstIndex + 1);
//        //channel interest contains only one ":"
//        //news:newsgn:gdxw:0.4541,news:0.2726,news:newsgn:0.2762,zj:news:0.7541,zj:0.1662 20131209
//        if (secondIndex < 0)
//        {
//            return firstIndex;
//        }
//        return -1;
//    }

//    @Override

    /**
     * This method gives a name to the column.
     *
     * @param input - schema of the input data
     * @return schema of the input data
     */
//    public Schema outputSchema(Schema input)
//    {
//        Schema bagSchema = new Schema();
//        bagSchema.add(new Schema.FieldSchema("column", DataType.CHARARRAY));
//        try
//        {
//            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
//                    bagSchema, DataType.BAG));
//        }
//        catch (FrontendException e)
//        {
//            return null;
//        }
//    }

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
