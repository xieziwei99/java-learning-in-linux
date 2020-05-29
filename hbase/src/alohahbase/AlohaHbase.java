package alohahbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 创建如下表结构
 * ------------------------------------
 * | 行健 |        列族 + 列名          |
 * ------------------------------------
 * | name |          score            |
 * -      -----------------------------
 * |      | English | Math | Computer |
 * ------------------------------------
 *
 * @author xzw
 * 2020-05-29
 */
public class AlohaHbase {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void main(String[] args) throws IOException {
        createTable("student", "score");
    }

    public static void init() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
        configuration.set("hadoop.home.dir", "hdfs://localhost:9000");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createTable(String tableName, String... colFamilies) throws IOException {
        init();
        TableName tableNameObject = TableName.valueOf(tableName);
        if (admin.tableExists(tableNameObject)) {
            System.out.println(tableName + " 表已存在");
            return;
        }
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObject);
        List<ColumnFamilyDescriptor> familyDescriptorList = new ArrayList<>();
        for (String colFamily : colFamilies) {
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(colFamily)).build();
            familyDescriptorList.add(columnFamilyDescriptor);
        }
        TableDescriptor tableDescriptor = tableDescriptorBuilder.setColumnFamilies(familyDescriptorList).build();
        admin.createTable(tableDescriptor);
        close();
        System.out.println(tableName + "表添加成功");
    }
}
