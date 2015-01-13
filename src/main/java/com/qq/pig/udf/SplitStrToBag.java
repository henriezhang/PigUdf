package com.qq.pig.udf;

//import com.google.common.base.Splitter;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;

/**
 * Created by antyrao on 14-3-5.
 */
public class SplitStrToBag extends EvalFunc<DataBag>
{
    String delimiter = ",";

    public SplitStrToBag()
    {
    }

    public SplitStrToBag(String delimiter)
    {
        this.delimiter = delimiter;
    }


    @Override
    public DataBag exec(Tuple input) throws IOException
    {
        if (input == null || input.size() == 0)
        {
            return null;
        }
        try
        {

            String str = (String) input.get(0);
            if (str == null)
            {
                return null;
            }
            String[] strs = str.split(delimiter);
//            List<String> strs = Splitter.on(delimiter).splitToList(str);


            // The assumption is that if the bag contents fits into
            // an input tuple, it will not need to be spilled.
            DataBag bag = new NonSpillableDataBag(strs.length);

            for (int i = 0; i < strs.length; ++i)
            {
                final String item = strs[i];
                Tuple tuple = TupleFactory.getInstance().newTuple(1);
                tuple.set(0, item);
                bag.add(tuple);
            }

            return bag;
        }
        catch (Exception ee)
        {
            throw new RuntimeException("Error while creating a bag", ee);
        }
    }

    public static void main(String[] args)
    {
//        List<String> t = Splitter.on("+").splitToList("+12+12+");
//        System.out.println(t);

    }

//    /* (non-Javadoc)
//     * @see org.apache.pig.EvalFunc#outputSchema(org.apache.pig.impl.logicalLayer.schema.Schema)
//     * If all the columns in the tuple are of same type, then set the bag schema
//     * to bag of tuple with column of this type
//     *
//     */
//    @Override
//    public Schema outputSchema(Schema inputSch)
//    {
//        byte type = DataType.ERROR;
//        Schema innerSchema = null;
//        if (inputSch != null)
//        {
//            for (Schema.FieldSchema fs : inputSch.getFields())
//            {~
//                if (type == DataType.ERROR)
//                {
//                    type = fs.type;
//                    innerSchema = fs.schema;
//                }
//                else
//                {
//                    if (type != fs.type || !nullEquals(innerSchema, fs.schema))
//                    {
//                        // invalidate the type
//                        type = DataType.ERROR;
//                        break;
//                    }
//                }
//            }
//        }
//        try
//        {
//            if (type == DataType.ERROR)
//            {
//                return Schema.generateNestedSchema(DataType.BAG, DataType.NULL);
//            }
//            Schema.FieldSchema innerFs = new Schema.FieldSchema(null, innerSchema, type);
//            Schema innerSch = new Schema(innerFs);
//            Schema bagSchema = new Schema(new Schema.FieldSchema(null, innerSch, DataType.BAG));
//            return bagSchema;
//        }
//        catch (FrontendException e)
//        {
//            //This should not happen
//            throw new RuntimeException("Bug : exception thrown while " +
//                    "creating output schema for TOBAG udf", e);
//        }
//
//    }

//    private boolean nullEquals(Schema currentSchema, Schema newSchema)
//    {
//        if (currentSchema == null)
//        {
//            if (newSchema != null)
//            {
//                return false;
//            }
//            return true;
//        }
//        return Schema.equals(currentSchema, newSchema, false, true);
//    }


}
