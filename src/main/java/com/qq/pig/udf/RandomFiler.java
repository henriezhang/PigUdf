package com.qq.pig.udf;

import org.apache.pig.FilterFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
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
 * Date: 13-11-13
 * Time: 上午11:03
 */
public class RandomFiler extends FilterFunc
{

    private Pattern idPattern = Pattern.compile("^\\d+$");


    public Boolean exec(Tuple arg0) throws IOException
    {
        if (arg0 == null || arg0.size() == 0)
            return false;

        String qqId;
        try
        {
            qqId = (String) arg0.get(0);
            if (qqId == null)
                return false;
            qqId = qqId.trim();
        }
        catch (Exception e)
        {
            System.err.println("NonURLDetector: failed to process input; error - " + e.getMessage());
            return false;
        }

        if (qqId.equals(""))
        {
            return false;
        }

        Matcher m = idPattern.matcher(qqId);
        return m.find();
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
