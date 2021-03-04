package com.csxm.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

public class HbaseHelp {
    private static Logger logger = LoggerFactory.getLogger(HbaseHelp.class);

    private Configuration config;
    private Connection connection = null;

    public HbaseHelp(String quorum, String port) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", quorum);
        conf.set("hbase.zookeeper.property.clientPort", port);
        config = conf;
    }

    public Connection getConnection() throws IOException {
        if (connection == null || connection.isClosed() || connection.isAborted()) {
            connection = ConnectionFactory.createConnection(config);
        }
        return connection;
    }

    public boolean isTableExist(String tablename) throws IOException {
        return getConnection().getAdmin().tableExists(TableName.valueOf(tablename));
    }

    public Table getTable(String tablename) throws IOException {
        if(isTableExist(tablename)){
            return getConnection().getTable(TableName.valueOf(tablename));
        }else{
            return null;
        }
    }

    public void createTable(String tablename,String...columnFamily) throws IOException {
        if(!isTableExist(tablename)){
            Admin admin = getConnection().getAdmin();

            // 创建集合用于存放ColumnFamilyDescriptor对象
            List<ColumnFamilyDescriptor> families = new ArrayList<>();
            // 将每个familyName对应的ColumnFamilyDescriptor对象添加到families集合中保存
            //创建多个列族
            for (String familyName : columnFamily){
                families.add(ColumnFamilyDescriptorBuilder.newBuilder(familyName.getBytes()).build());
            }
            // 构建TableDescriptor对象，以保存tableName与familyNames
            TableDescriptor descriptor = TableDescriptorBuilder.newBuilder((TableName.valueOf(tablename))).setColumnFamilies(families).build();
            admin.createTable(descriptor);
        }
    }

    public byte[] getutf8bytes(String str) {
        try {
            return str.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    public void putAddCol(Put put,String family, String key, String value) {
        put.addColumn(getutf8bytes(family), getutf8bytes(key), getutf8bytes(value));
    }

    public Put getPut(String rowkey) {
        return new Put(getutf8bytes(rowkey));
    }

    public Get getGet(String rowkey) {
        return new Get(getutf8bytes(rowkey));
    }

    public Delete getDelete(String rowkey) {
        return new Delete(getutf8bytes(rowkey));


    }


    public Map<String, String> getRowMap(Result result,String family) {
        Map<String, String> map = new HashMap();
        if (result == null) {
            return map;
        }
        String rowkey = Bytes.toString(result.getRow());

        map.put("rowkey", rowkey);
        NavigableMap<byte[], byte[]> m = result.getFamilyMap(getutf8bytes(family));
        if (m == null) {
            return map;
        }

        m.forEach((k, v) -> {
            map.put(Bytes.toString(k), Bytes.toString(v));

        });

        return map;
    }

    public List<Map<String, String>> getListRowMap(ResultScanner scanner,String family) {
        List<Map<String, String>> list = new ArrayList();
        scanner.forEach(scan -> {
            list.add(getRowMap(scan,family));
        });

        return list;

    }


    private long getCount(String table, String prex) throws Exception {
        Scan countScan = new Scan();
        List<Filter> countFilters = new ArrayList();
        if (StringUtils.isNotBlank(prex)) {
            Filter filter = new RowFilter(CompareOperator.EQUAL, new SubstringComparator(prex));
            countFilters.add(filter);
        }
        countFilters.add(new FirstKeyOnlyFilter());
        FilterList countFilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, countFilters);
        countScan.setFilter(countFilterList);
        countScan.setCaching(500);
        countScan.setCacheBlocks(false);
        ResultScanner countScanner = getTable(table).getScanner(countScan);
        long totalSize = 0;
        for (Result result : countScanner) {
            totalSize++;
        }
        return totalSize;
    }

}
