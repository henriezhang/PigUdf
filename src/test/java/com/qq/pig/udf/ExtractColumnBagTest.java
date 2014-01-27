package com.qq.pig.udf;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.util.Collection;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: antyrao
 * Date: 13-12-19
 * Time: 下午4:27
 */
public class ExtractColumnBagTest
{
    public static void main(String[] args)
    {
//        sports.isocce.yingc.liverp
//        SportColumnDict dict = new SportColumnDict();
//        List<String> columns = Lists.newArrayList();
//        columns.add("sports");
//        columns.add("isocce");
//        columns.add("yingc");
//        columns.add("liverp");
//
//        int index = 1;//skip first column : sports
//        for (; index < columns.size(); index++)
//        {
//            String column = columns.get(index);
//            Collection<String> mappedColumns = dict.getMappedColumn(column);
//            for (String mappedColumn : mappedColumns)
//            {
//                System.out.println(mappedColumn);
////                Tuple t = TupleFactory.getInstance().newTuple(1);
////                t.set(0, mappedColumn);
////                output.add(t);
//            }
//        }


        float weight = 0.00001f;
        int normalizedWeight = Math.round(weight * 100);
        if (normalizedWeight > 0)
        {
            System.out.println(normalizedWeight);
            return;

        }
//        Preconditions.checkState(weight == 0);
        if (weight > 0)
        {
            System.out.println(1);
            return;
        }

        System.out.println(0);


    }
}
