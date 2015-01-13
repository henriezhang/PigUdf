package com.qq.pig.udf;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: antyrao
 * Date: 13-11-12
 * Time: 下午4:49
 * <p/>
 * <p/>
 * this class used to extract a fixed field from a comma-delimited line
 * <p>then expand the field to tuple of fixed size
 */
public class VideoAppIPhoneToTuple extends EvalFunc<Tuple>
{
    private static final int TUPLE_SIZE = 3;

    private static final ObjectMapper mapper = new ObjectMapper();

    public static Optional<String> extractLastField(String line)
    {
        int index = line.indexOf(",{");
        if (index == -1)
            return Optional.absent();
        String strJson = line.substring(index + 1, line.length());
        strJson = strJson.replace("\\,", ",");
        return Optional.of(strJson);
    }

    public static Optional<Map<?, ?>> extractFieldToMap(String line) throws IOException
    {
        Optional<String> optional = extractLastField(line);
        if (optional.isPresent())
            return Optional.<Map<?, ?>>of(mapper.readValue(optional.get(), Map.class));
        return Optional.absent();
    }

    public static List<Object> toList(String line) throws IOException
    {
        List<Object> result = Lists.newArrayList();

        Optional<Map<?, ?>> optional = extractFieldToMap(line);
        if (!optional.isPresent())
        {
            return null;
        }
        Map<?, ?> jsonStr = optional.get();

        String deviceid = null;
        String guid = null;
        String movieid = null;
        Map<?, ?> kv = (Map<?, ?>) jsonStr.get(KV);
        if (kv != null)
        {
            deviceid = (String) kv.get(KV_DEVICE_ID);
            guid = (String) kv.get(KV_GUID);
            movieid = (String) kv.get(KV_MOVIE_ID);
        }
        result.add(deviceid);
        result.add(guid);
        result.add(movieid);

        return result;
    }


    //common fields
    private static final String EI = "ei";
    private static final String ET = "et";

    //    private static final String UI = "ui";
    private static final String KV = "kv";
    private static final String KV_DEVICE_ID = "deviceID";
    private static final String KV_GUID = "guid";
    private static final String KV_MOVIE_ID = "movieid";
//    private static final String KV_DEVICE_ID = "deviceID";
//    private static final String KV_DEVICE_ID = "deviceID";

//    private static final String KV_VOID_LIVE_CDNID = "vod_live_cdnId";
//    private static final String KV_VOID_LIVE_CDNID = "vod_live_cdnId";
//    private static final String KV_VOID_LIVE_CDNID = "vod_live_cdnId";
//    private static final String KV_VOD_LIVE_PLAYERID = "vod_live_playerId";
    private static final String ID = "id";

    //fields:ui,vod_live_cdnId,vod_live_playerId,ei,et,id
    public Tuple exec(Tuple input) throws IOException
    {
        if (input == null || input.size() == 0)
        {
            return null;
        }
        try
        {
            String line = (String) input.get(0);
            if (Strings.isNullOrEmpty(line))
            {
                return null;
            }
            Tuple t = TupleFactory.getInstance().newTuple(TUPLE_SIZE);
            List<Object> result = toList(line);
            for (int i = 0; i < result.size(); i++)
            {
                t.set(i, result.get(i));
            }
            return t;
        }
        catch (Exception e)
        {
            System.err.println("VideoAppToTuple: failed to process input; error - " + e.getMessage() + Throwables.getStackTraceAsString(e));
            return null;
        }
    }

    public static int getWeight(String channelInterest, int index)
    {
        String strWeight = channelInterest.substring(index + 1);
        Float weight = Float.valueOf(strWeight);
        return Utils.roundUpToInt(weight);
    }


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
        //fields:ui,vod_live_cdnId,vod_live_playerId,ei,et,id
        Schema tupleSchema = new Schema();
        tupleSchema.add(new Schema.FieldSchema("ei", DataType.CHARARRAY));
        tupleSchema.add(new Schema.FieldSchema("et", DataType.LONG));
        tupleSchema.add(new Schema.FieldSchema("deviceid", DataType.CHARARRAY));
        tupleSchema.add(new Schema.FieldSchema("guid", DataType.CHARARRAY));
        tupleSchema.add(new Schema.FieldSchema("movieid", DataType.CHARARRAY));
        tupleSchema.add(new Schema.FieldSchema("id", DataType.LONG));
//        tupleSchema.add(new Schema.FieldSchema("ei", DataType.CHARARRAY));
//        tupleSchema.add(new Schema.FieldSchema("et", DataType.LONG));
        try
        {
            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                    tupleSchema, DataType.TUPLE));
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
