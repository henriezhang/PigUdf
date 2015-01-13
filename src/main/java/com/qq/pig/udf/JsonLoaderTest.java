package com.qq.pig.udf;

import org.apache.hadoop.io.Text;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: antyrao
 * Date: 14-1-1
 * Time: 上午8:31
 */
public class JsonLoaderTest
{
    public static void main(String[] args) throws IOException
    {
//        CustomJsonLoader loader = new CustomJsonLoader("ar:{(ar1:chararray)},cfg:[chararray],du:long,ea:long,ei:chararray,er:chararray,et:long,ev:(tn:long,os:chararray,ov:chararray,op:chararray,sr:chararray,sd:chararray,im:chararray,av:chararray,jb:long,tz:chararray,cn:chararray,md:chararray,lg:chararray,sv:chararray,mf:chararray,ch:chararray,lc:chararray),eva:chararray,id:long,ip:chararray,kv:[chararray],ky:chararray,mc:chararray,pi:chararray,qq:chararray,si:long,tf:(ur:long,ut:long,mr:long,tt:long,tr:long,dy:long,mt:long),ts:long,ui:chararray,ifv:chararray,ifa:chararray");
//        Text value = new Text("{\"ui\":\"a00000429de6e1\",\"ky\":\"ATXRSY245Y54\",\"ev\":{\"tn\":6,\"os\":1,\"ov\":\"15\",\"op\":\"46003\",\"sr\":\"480*800\",\"sd\":\"6862/7928\",\"av\":\"3.2.2\",\"tz\":\"Asia/Chongqing\",\"cn\":\"WIFI\",\"md\":\"HUAWEI C8826D\",\"lg\":\"zh\",\"sv\":\"1.3.0\",\"mf\":\"HUAWEI\",\"ch\":\"127\"},\"ts\":1388332752,\"mc\":\"5c:7d:5e:78:19:82\",\"cfg\":{\"2\":{},\"1\":{}},\"ut\":1,\"et\":2,\"si\":1368243591,\"ip\":\"110.155.81.84\",\"id\":1100671636}\n");
//        Text value = new Text("{\"ui\":\"860311020650677\",\"ky\":\"A3LFCJD983W9\",\"ts\":1388332750,\"mc\":\"c4:6a:b7:d2:78:cb\",\"ei\":\"boss_app_normal_start\",\"ar\":[\"860311020650677\",\"ea248995-d29a-4b57-b86a-3ab11a02e2f8\"],\"et\":1000,\"si\":77949954,\"ip\":\"221.11.12.108\",\"id\":1100032544}");
//        Text value = new TextS("{\"ui\":\"357513050040832\",\"ar\":[],\"et\":1000,\"si\":1737810905,\"ip\":\"119.123.224.15\",\"id\":1100032544}\n");

//        Text value = new Text("{\"ui\":\"860311020650677\",\"ar\":[\"860311020650677\",\"ea248995-d29a-4b57-b86a-3ab11a02e2f8\"],\"et\":1000}");
//
//        Tuple tuple = loader.parseTuple(value);
//        System.out.println(tuple.toString());

    }
}
