package com.analyse.site;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

public class HbaseUtils {
    HBaseAdmin admin = null;
    Configuration conf =null;

    private  HbaseUtils() {
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "s01:2181");
        conf.set("hbase.rootdir", "hdfs://s00:9000/hbase");

        try {
            admin = new HBaseAdmin(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static HbaseUtils instance = null;

    public static  HbaseUtils getInstance() {
        if(instance == null)
        {
            instance = new HbaseUtils();
        }
        return instance;
    }

    public HTable getTable(String tableName)
    {
        HTable table = null;
        try {
            table = new HTable(conf, tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return table;
    }

    public void put(String tableName, String rowkey, String cf, String column, String value)
    {
        HTable table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Map<String,Long> query(String tableName, String condition) throws Exception
    {
        Map<String,Long> map = new HashMap<String,Long>();
        HTable table = getTable(tableName);

        String cf = "info";
        String qualifier = "log_count";

        Scan scan = new Scan();

        Filter filter = (Filter) new PrefixFilter(Bytes.toBytes(condition));
        scan.setFilter((org.apache.hadoop.hbase.filter.Filter) filter);

        ResultScanner rs = table.getScanner(scan);
        for (Result result : rs)
        {
            String row = Bytes.toString(result.getRow());
            long clickCount = Bytes.toLong(result.getValue(cf.getBytes(), qualifier.getBytes()));
            long clickCount1  =  Long.valueOf(clickCount);
            map.put(row, clickCount1);
        }
        return map;
    }

    public static void main(String args[]) throws Exception
    {
        Map<String,Long > map =HbaseUtils.getInstance().query("search_log_count","20180522");

        for (Map.Entry<String, Long> entry : map.entrySet())
        {
            System.out.println(entry.getKey()+":"+entry.getValue());
        }
    }


}
