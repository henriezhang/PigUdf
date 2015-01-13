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

/**
 * Created with IntelliJ IDEA.
 * User: antyrao
 * Date: 13-11-15
 * Time: 下午3:57
 */
public class GuidPrefixFilter extends FilterFunc
{

    private String filter;

    public GuidPrefixFilter(String filter)
    {
        this.filter = filter;
    }

    public Boolean exec(Tuple arg0) throws IOException
    {
        if (arg0 == null || arg0.size() == 0)
            return false;

        String guid = (String) arg0.get(0);
        if (guid == null)
            return false;

        String prefix = guid.substring(0, 1);
        return filter.contains(prefix);

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
