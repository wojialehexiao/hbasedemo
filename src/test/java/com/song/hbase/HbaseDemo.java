package com.song.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class HbaseDemo {

    static Configuration config = null;
    private Connection connection = null;
    private Table table;

    @Before
    public void init() throws IOException {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","master,slave,slave1");
        config.set("hbase.zookeeper.property.clientPort","2181");
        connection = ConnectionFactory.createConnection(config);
        table = connection.getTable(TableName.valueOf("user"));
    }

    @Test
    public void createTable() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(config);
        TableName tableName = TableName.valueOf("test3");
        HTableDescriptor desc = new HTableDescriptor(tableName);
        HColumnDescriptor family = new HColumnDescriptor("info");
        desc.addFamily(family);

        HColumnDescriptor family2 = new HColumnDescriptor("info2");
        desc.addFamily(family2);

        admin.createTable(desc);
    }

    @Test
    public void insertDate() throws IOException {
        Put put = new Put(Bytes.toBytes("wang_123"));
        put.add(Bytes.toBytes("info2"),Bytes.toBytes("name"),Bytes.toBytes("wang"));
        put.add(Bytes.toBytes("info2"),Bytes.toBytes("sex"),Bytes.toBytes("nan"));
        put.add(Bytes.toBytes("info2"),Bytes.toBytes("age"),Bytes.toBytes(23));
//        table.put(put);

        Put put1 = new Put(Bytes.toBytes("wang_124"));
        put1.add(Bytes.toBytes("info2"),Bytes.toBytes("name"),Bytes.toBytes("wang"));
        put1.add(Bytes.toBytes("info2"),Bytes.toBytes("sex"),Bytes.toBytes("nan"));
        put1.add(Bytes.toBytes("info2"),Bytes.toBytes("age"),Bytes.toBytes(23));

        List<Put> list = new ArrayList<Put>();
        list.add(put);
        list.add(put1);

        table.put(list);
    }

    @Test
    public void delete() throws IOException {
        Delete delete = new Delete(Bytes.toBytes("song_124"));
        delete.addFamily(Bytes.toBytes("info1"));
        table.delete(delete);
    }

    @Test
    public void deleteColumn() throws IOException {
        Delete delete = new Delete(Bytes.toBytes("song_124"));
        delete.addColumn(Bytes.toBytes("info2"),Bytes.toBytes("name"));
        table.delete(delete);
    }

    @Test
    public void queryData() throws IOException {
        Get get = new Get(Bytes.toBytes("song_123"));
        Result result = table.get(get);
        byte[] name = result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name"));
        byte[] sex = result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("sex"));
        byte[] age = result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("age"));
        System.out.println(Bytes.toString(name));
        System.out.println(Bytes.toString(sex));
        System.out.println(Bytes.toInt(age));

    }

    @Test
    public void scan() throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("song"));
        scan.setStopRow(Bytes.toBytes("t"));
        scan.addFamily(Bytes.toBytes("info2"));

        ResultScanner scanner = table.getScanner(scan);

        for(Result result : scanner){
            byte[] name = result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name"));
            byte[] sex = result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("sex"));
            byte[] age = result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("age"));
            System.out.println(Bytes.toString(result.getRow()) + "---------------");
            System.out.println(Bytes.toString(name));
            System.out.println(Bytes.toString(sex));
            System.out.println(Bytes.toInt(age));
        }
    }


    @Test
    public void scanByFilter() throws IOException {
        Scan scan = new Scan();
        SingleColumnValueExcludeFilter filter =
                new SingleColumnValueExcludeFilter(Bytes.toBytes("info2"),Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL,Bytes.toBytes("song"));

        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);

        for(Result result : scanner){
            byte[] name = result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name"));
            byte[] sex = result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("sex"));
            byte[] age = result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("age"));
            System.out.println(Bytes.toString(result.getRow()) + "---------------");
            System.out.println(Bytes.toString(name));
            System.out.println(Bytes.toString(sex));
            System.out.println(Bytes.toInt(age));
        }
    }

    @After
    public void destroy() throws IOException {
        if(connection != null){
            connection.close();
        }

        if(table !=null) {
            table.close();
        }
    }
}
