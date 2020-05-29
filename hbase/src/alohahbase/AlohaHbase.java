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
        insertData("student", "zhangsan", "score", "English", "69");
        insertData("student", "zhangsan", "score", "Math", "86");
        insertData("student", "zhangsan", "score", "Computer", "100");
        String computerScore = new String(getData("student", "zhangsan", "score", "Computer"));
        System.out.println("张三的计算机分数为 " + computerScore);
    }

    /**
     * Hbase 初始化 创建 configuration connection admin
     */
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

    /**
     * Hbase 关闭 connection admin
     */
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

    /**
     * 创建 Hbase 表
     *
     * @param tableName   表名
     * @param colFamilies 列族名，可变参数
     */
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

    /**
     * 插入一条数据
     *
     * @param tableName 表名
     * @param rowKey    行健
     * @param colFamily 列族
     * @param col       列
     * @param value     值
     * @throws IOException 如果表不存在，会抛出如 org.apache.hadoop.hbase.TableNotFoundException: student
     *                     其他原因如网络等也会抛出异常
     */
    public static void insertData(String tableName, String rowKey, String colFamily, String col, String value) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));    // 表不存在会抛出异常
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(value));
        table.put(put);
        table.close();
        close();
        System.out.println("插入数据成功");
    }

    /**
     * 获取数据
     *
     * @param tableName 表名
     * @param rowKey    行健
     * @param colFamily 列族 如果为空，会出现空指针异常
     * @param col       列 如果为空，会出现空指针异常
     * @return 该单元格的值
     * @throws IOException 如果表不存在，会抛出如 org.apache.hadoop.hbase.TableNotFoundException: student
     *                     其他原因如网络等也会抛出异常
     */
    public static byte[] getData(String tableName, String rowKey, String colFamily, String col) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));    // 表不存在会抛出异常
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        Result result = table.get(get);
        byte[] value = result.getValue(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        table.close();
        close();
        return value;
    }
}
