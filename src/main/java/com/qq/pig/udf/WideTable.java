package com.qq.pig.udf;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

/**
 * the purpose of this UDF is to create wide table from bag of tuples.
 * e.g.:
 * id 1 0.1
 * id 2 0.2
 * ||
 * id 0.1 02
 * <p/>
 * User: antyrao
 * Date: 13-11-12
 * Time: 下午4:49
 */
public class WideTable extends EvalFunc<Tuple>
{
//    private SportColumnDict dict = new SportColumnDict();
//    private int count = dict.getColumns().size();
//

    private static final String DEFAULT_DICT_NAME = "dict";
    private String dictName = null;
    private HashMap<String, Integer> dictLookUp = null;
    private int count;

    public WideTable()
    {
        this.dictName = DEFAULT_DICT_NAME;
    }

    public WideTable(String dictName)
    {
        this.dictName = dictName;
    }

    private void init() throws IOException
    {
        dictLookUp = new HashMap<String, Integer>();
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("./" + dictName), "utf-8"));
        String line;
        int cnt = 0;
        while ((line = br.readLine()) != null)
        {
            String[] tmp = line.split("\\s+");
            dictLookUp.put(tmp[0], cnt++);
        }
        this.count = dictLookUp.size();
        br.close();
    }


    public Tuple exec(Tuple input) throws IOException
    {
        if (input == null || input.size() == 0)
            return null;

        try
        {
            if (dictLookUp == null)
            {
                init();
            }
            Tuple output = TupleFactory.getInstance().newTuple(count);
            DataBag dataBag = (DataBag) input.get(0);
            if (dataBag == null)
            {
                return null;
            }
            Utils.populateWithZero(output);

            for (Tuple tuple : dataBag)
            {
                if (tuple.size() < 3)
                    continue;
                String columnName = (String) tuple.get(1);
                Double interest = (Double) tuple.get(2);
                if (Strings.isNullOrEmpty(columnName) || interest == null)
                    continue;
                Integer position = dictLookUp.get(columnName);
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
