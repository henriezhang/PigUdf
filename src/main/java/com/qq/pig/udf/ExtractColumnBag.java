package com.qq.pig.udf;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: antyrao
 * Date: 13-11-12
 * Time: 下午4:49
 */
public class ExtractColumnBag extends EvalFunc<DataBag>
{

    private Splitter commaSplitter = Splitter.on(",");
    private SportColumnDict dict = new SportColumnDict();
    private ColumnParser parser = new ColumnParser();
    private static final String SPORTS = "sports";


    public DataBag exec(Tuple input) throws IOException
    {
        if (input == null || input.size() == 0)
        {
            return null;
        }
        try
        {
            DataBag output = DefaultBagFactory.getInstance().newDefaultBag();
            String columnStr = (String) input.get(0);
            if (Strings.isNullOrEmpty(columnStr))
            {
                return null;
            }
            List<String> columns = parser.parseColumns(columnStr);
            if (columns.size() == 0 || !columns.get(0).equals(SPORTS))//only accept sports column
            {
                return null;
            }
            int index = 1;//skip first column : sports
            for (; index < columns.size(); index++)
            {
                String column = columns.get(index);
                Collection<String> mappedColumns = dict.getMappedColumn(column);
                for (String mappedColumn : mappedColumns)
                {
                    Tuple t = TupleFactory.getInstance().newTuple(1);
                    t.set(0, mappedColumn);
                    output.add(t);
                }
            }
            return output;
        }
        catch (Exception e)
        {
            System.err.println("ExtractChannelBag: failed to process input; error - " + e.getMessage() /*+ "," + Throwables.getStackTraceAsString(e)*/);
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
//    @Override
//    public List<FuncSpec> getArgToFuncMapping() throws FrontendException
//    {
//        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
//        funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));
//
//        return funcList;
//    }
}
