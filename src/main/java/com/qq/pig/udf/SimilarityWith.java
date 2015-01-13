package com.qq.pig.udf;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by antyrao on 14-7-8.
 */
public class SimilarityWith extends EvalFunc<Double>
{
    private String baseStr = null;
    private boolean initialized = false;
    private Integer baseTopLevelTag;
    private Set<Integer> baseSecondLevelTags = null;
    private Set<Integer> baseAreaTags = null;
    private Set<String> baseDirectorTags = null;
    private Set<String> baseActorTags = null;
    private Set<String> baseGuestTags = null;


    private static final double topLevelWeight = 0.1;
    private static final double secondLevelWeight = 0.3;
    private static final double areaWeight = 0.1;
    private static final double directorWeight = 0.1;
    private static final double actorWeight = 0.2;
    private static final double guestWeight = 0.2;


    private static final int TOP_LEVEL_SIZE = 55;
    private static final int AREA_SIZE = 93;
    private static Map<Integer, Integer> TYPE_SIZE = Maps.newHashMap();

    static
    {
        TYPE_SIZE.put(Integer.valueOf("1"), Integer.valueOf("32"));
        TYPE_SIZE.put(Integer.valueOf("2"), Integer.valueOf("29"));
        TYPE_SIZE.put(Integer.valueOf("10"), Integer.valueOf("29"));
        TYPE_SIZE.put(Integer.valueOf("3"), Integer.valueOf("31"));
    }


    public SimilarityWith()
    {
    }

    public SimilarityWith(String baseStr)
    {
        this.baseStr = baseStr;
    }

    public void init()
    {
        //3;20,22,256,18;
        //typeid:subtypeid:area:director:actor:guest
        String[] tmp = baseStr.split(";");
        baseTopLevelTag = Integer.valueOf(tmp[0]);
        baseSecondLevelTags = toIntSet(tmp[1], ",");
        baseAreaTags = tmp.length < 3 ? null : toIntSet(tmp[2], "\\+");
        baseDirectorTags = tmp.length < 4 ? null : toStrSet(tmp[3], ",");
        baseActorTags = tmp.length < 5 ? null : toStrSet(tmp[4], ",");
        if (tmp.length > 5)
        {
            baseGuestTags = toStrSet(tmp[5], ",");
        }

        initialized = true;
    }

    private static Set<Integer> toIntSet(String str, String separator)
    {
        if (Strings.isNullOrEmpty(str))
            return null;
        try
        {
            Set<Integer> result = Sets.newHashSet();
            String[] temps = str.split(separator);
            for (String tmp : temps)
            {
                if (tmp.isEmpty() || tmp.equals(""))
                    continue;
                result.add(Integer.valueOf(tmp));
            }
            return result;
        }
        catch (Throwable throwable)
        {
            return null;
        }
    }

    private static Set<String> toStrSet(String str, String separator)
    {
        if (Strings.isNullOrEmpty(str))
        {
            return null;
        }
        try
        {
            Set<String> result = Sets.newHashSet();
            String[] temps = str.split(separator);
            for (String tmp : temps)
            {
                if (tmp.isEmpty() || tmp.equals(""))
                    continue;
                result.add(tmp);
            }
            return result;
        }
        catch (Throwable throwable)
        {
            return null;
        }
    }

    @Override
    public Double exec(Tuple input) throws IOException
    {
        if (input == null || input.size() == 0)
        {
            return null;
        }
        if (!initialized)
        {
            init();
        }
        try
        {
            DataBag bag = (DataBag) input.get(0);
            if (bag == null)
                return null;
            Iterator<Tuple> iter = bag.iterator();
            Tuple t = iter.next();

            String vid = (String) t.get(0);
            String topLevel = (String) t.get(1);
            if (topLevel == null)
            {
                return null;
            }
            Integer topLevelTags = Integer.valueOf(topLevel);
            String secondLevel = (String) t.get(2);
            String area = (String) t.get(3);
            String director = (String) t.get(4);
            String actor = (String) t.get(5);
            String guest = (String) t.get(6);
            return computeSimilarity(topLevelTags, toIntSet(secondLevel, ","), toIntSet(area, "\\+"),
                    toStrSet(director, ","), toStrSet(actor, ","), toStrSet(guest, ","));
        }
        catch (NumberFormatException e)
        {
            return null;
        }
        catch (Exception ee)
        {
            throw new RuntimeException("Error while creating a bag", ee);
        }
    }

    public double computeSimilarity(Integer topLevelTags, Set<Integer> secondLevelTags, Set<Integer> areaTags,
                                    Set<String> director, Set<String> actor, Set<String> guest)
    {
        double total = 0;
        /**
         * top level are
         */
        if (baseTopLevelTag.equals(topLevelTags))
        {
            total = total + topLevelWeight;
            if (secondLevelTags != null)
            {
                int shared = Sets.intersection(baseSecondLevelTags, secondLevelTags).size();
                if (shared > 0)
                {
                    double denominator = Math.sqrt(baseSecondLevelTags.size()) * Math.sqrt(secondLevelTags.size());

                    total = total + ((double) shared) / denominator * secondLevelWeight;
                }
            }
        }

        if (areaTags != null && baseAreaTags != null)
        {
            int areaShared = Sets.intersection(baseAreaTags, areaTags).size();
            if (areaShared > 0)
            {
                double denominator = Math.sqrt(baseAreaTags.size()) * Math.sqrt(areaTags.size());
                total = total + ((double) areaShared) / denominator * areaWeight;
            }
        }

        if (director != null && baseDirectorTags != null)
        {
            int directorShared = Sets.intersection(baseDirectorTags, director).size();
            if (directorShared > 0)
            {
                total = total + (double) directorShared / baseDirectorTags.size() * directorWeight;
            }
        }

        if (actor != null && baseActorTags != null)
        {
            int actorShared = Sets.intersection(baseActorTags, actor).size();
            if (actorShared > 0)
            {
                total = total + (double) actorShared / baseActorTags.size() * actorWeight;
            }
        }

        if (guest != null && baseGuestTags != null)
        {
            int guestShared = Sets.intersection(baseGuestTags, guest).size();
            if (guestShared > 0)
            {
                total = total + (double) guestShared / baseGuestTags.size() * guestWeight;
            }
        }

        return total;
    }

    public static void main(String[] args)
    {
        SimilarityWith sim = new SimilarityWith("1;3,16,1048576;153505+;赵洋;薛赫,陈雯君;");
        sim.init();
        String test = "1;2,3;;;";
        Set<Integer> secondLevelTags = new HashSet<Integer>();
        secondLevelTags.add(2);
        secondLevelTags.add(3);
        Double rst = sim.computeSimilarity(1, secondLevelTags, null, null, null, null);
        System.out.println(rst);
    }
}
