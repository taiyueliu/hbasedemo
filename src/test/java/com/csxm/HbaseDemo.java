package com.csxm;

import com.csxm.utils.HbaseHelp;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/**
 * @description: TODO
 * @author: liutaiyue
 * @time: 2021/3/4 14:58
 * @Version 1.0
 */
public class HbaseDemo {
    private static HbaseHelp hbaseHelp = null;
    public static void init(){
         hbaseHelp = new HbaseHelp("192.168.10.65,192.168.10.75,192.168.10.85","2181");
    }

    @Test
    public void createTable(){
        init();
        try {
            hbaseHelp.createTable("demo:stu","info","address");
            System.out.println("完成");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
