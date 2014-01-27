package com.qq.pig.udf;

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: antyrao
 * Date: 13-12-30
 * Time: 下午3:40
 */
public class JsonTest
{
    public static void main(String[] args) throws IOException
    {

        String jsonStr = "{\"ar\":[\"860311020650677\",\"ea248995-d29a-4b57-b86a-3ab11a02e2f8\"],\"et\":1000}";

        JsonFactory jsonFactory = new JsonFactory();

        ByteArrayInputStream bais = new ByteArrayInputStream(jsonStr.getBytes("utf-8"));

        JsonParser p = jsonFactory.createJsonParser(bais);

        if (p.nextToken() != JsonToken.START_OBJECT)
        {
            System.out.println("error");
            return;
        }

        p.nextToken();
        System.out.println("--" + p.getCurrentName());

        if (p.nextToken() != JsonToken.START_ARRAY)
        {
            System.out.println("START_ARRAY");
            return;
        }

        JsonToken innerTok;

        while ((innerTok = p.nextToken()) != JsonToken.END_ARRAY)
        {
            System.out.println(p.getText());
//            Tuple tuple = null;
//            if (innerTok == JsonToken.START_OBJECT)
//            {
//                tuple = (Tuple) readTuple(p, s, 0);
//            }
//            else
//            {
//                tuple = tupleFactory.newTuple(1);
//                tuple.set(0, readField(p, fs[0], 0));
//            }
//
//            bag.add(tuple);
        }

        p.nextToken();
        System.out.println("--" + p.getText());

        p.nextToken();
        System.out.println("--" + p.getText());

//        while (p.nextToken() != null)
//        {
//            System.out.println("-----------------");
////            System.out.println(p.getCurrentName() + "," + p.getText());
////            System.out.println();
////            p.nextToken();
//            System.out.println(p.getText());
////            System.out.println(p.getCurrentName() + "," + p.getText());
////            System.out.println(p.getText());
////            System.out.println(p.toString());
//            System.out.println("-----------------");
//        }


    }


}
