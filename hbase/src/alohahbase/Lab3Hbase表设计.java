package alohahbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 创建如下表结构
 * student 表
 * --------------------------------------------------------
 * | name |   info    |            score                  |
 * -      -------------------------------------------------
 * |      | sex | age | Math | Computer Science | English |
 * --------------------------------------------------------
 * <p>
 * course 表
 * -----------------
 * | name |  info  |
 * -      ----------
 * |      | credit |
 * -----------------
 *
 * @author xzw
 * 2020-05-29
 */
public class Lab3Hbase表设计 {

    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void main(String[] args) throws IOException {
        init();
        createTable("student", new String[]{"info", "score"});
        createTable("course", new String[]{"info"});
        addRecord("student", "Zhangsan",
                new String[]{"info:sex", "info:age", "score:Math", "score:English"},
                new String[]{"male", "23", "86", "69"});
        addRecord("student", "Mary",
                new String[]{"info:sex", "info:age", "score:Computer Science", "score:English"},
                new String[]{"female", "22", "77", "99"});
        addRecord("student", "Lisi",
                new String[]{"info:sex", "info:age", "score:Math", "score:Computer Science"},
                new String[]{"male", "24", "98", "95"});
        addRecord("course", "Math",
                new String[]{"info:credit"},
                new String[]{"2.0"});
        addRecord("course", "Computer Science",
                new String[]{"info:credit"},
                new String[]{"5.0"});
        addRecord("course", "English",
                new String[]{"info:credit"},
                new String[]{"3.0"});
        scanColumn("student", "info");
        scanColumn("student", "score:Math");
        scanColumn("student", "score:Computer Science");
        scanColumn("student", "score:English");
        scanColumn("course", "info:credit");
        modifyData("student", "Zhangsan", "score:Math", "100");
        scanColumn("student", "score:Math");
        deleteRow("student", "Lisi");
        scanColumn("student", "info");
        close();
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
     * 创建 Hbase 表<br>
     * 当HBase已经存在名为 tableName 的表的时候，先删除原有的表，然后再创建新的表
     *
     * @param tableName 表名
     * @param fields    列族名数组
     */
    public static void createTable(String tableName, String[] fields) throws IOException {
        TableName tName = TableName.valueOf(tableName);
        if (admin.tableExists(tName)) {
            System.out.println(tableName + " 表已存在, 准备删除 " + tableName + " 表");
            admin.disableTable(tName);
            admin.deleteTable(tName);
            System.out.println(tableName + " 表删除成功");
        }
        System.out.println("开始创建 " + tableName + " 表");
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tName);
        List<ColumnFamilyDescriptor> familyDescriptorList = new ArrayList<>();
        for (String colFamily : fields) {
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(colFamily)).build();
            familyDescriptorList.add(columnFamilyDescriptor);
        }
        TableDescriptor tableDescriptor = tableDescriptorBuilder.setColumnFamilies(familyDescriptorList).build();
        admin.createTable(tableDescriptor);
        System.out.println(tableName + " 表创建成功");
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
        Table table = connection.getTable(TableName.valueOf(tableName));    // 表不存在会抛出异常
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(value));
        table.put(put);
        table.close();
        System.out.println("向 " + tableName + " 表中插入数据成功");
    }

    /**
     * 向 tableName 表中插入多条数据
     *
     * @param fields 例如 {"info:sex", "info:age", "score:Math", "score:English"}
     * @throws IOException 如果表不存在，会抛出如 org.apache.hadoop.hbase.TableNotFoundException: student
     *                     其他原因如网络等也会抛出异常
     */
    public static void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
        if (fields.length != values.length) {
            System.out.println("参数 fields 和 values 不匹配");
            return;
        }
        System.out.println("开始向 " + tableName + " 表中插入 " + fields.length + " 条记录");
        for (int i = 0; i < fields.length; i++) {
            String[] split = fields[i].split(":");
            String colFamily = split[0].trim();
            String col = split[1].trim();
            String value = values[i];
            insertData(tableName, row, colFamily, col, value);
        }
        System.out.println(fields.length + " 条记录插入成功");
    }

    public static void scanColumn(String tableName, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        String[] split = column.split(":");
        Scan scan = new Scan();
        if (split.length == 2) {
            scan.addColumn(Bytes.toBytes(split[0].trim()), Bytes.toBytes(split[1].trim()));
            ResultScanner scanner = table.getScanner(scan);
            List<String> colValues = new ArrayList<>();
            for (Result result : scanner) {
                String value = Bytes.toString(result.getValue(Bytes.toBytes(split[0].trim()), Bytes.toBytes(split[1].trim())));
                colValues.add(value);
            }
            System.out.println(tableName + " 表中，" + column + " 这一列的值为 " + "\t" + colValues);
            scanner.close();
        } else {
            scan.addFamily(Bytes.toBytes(column));
            ResultScanner scanner = table.getScanner(scan);
            Map<String, List<String>> colValuesMap = new HashMap<>();
            for (Result result : scanner) {
                Map<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(column));
                for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                    colValuesMap.merge(
                            Bytes.toString(entry.getKey()),
                            new ArrayList<String>() {{
                                add(Bytes.toString(entry.getValue()));
                            }},
                            (oldList, newList) -> {
                                oldList.addAll(newList);
                                return oldList;
                            }
                    );
                }
            }
            colValuesMap.forEach((col, values) -> System.out.println(tableName + " 表中，" + column + ":" + col + " 这一列的值为 " + "\t" + values));
            scanner.close();
        }
        table.close();
    }

    public static void modifyData(String tableName, String row, String column, String newValue) throws IOException {
        String[] split = column.split(":");
        if (split.length != 2) {
            System.out.println("column 参数传入错误，无法定位单元格");
            return;
        }
        String colFamily = split[0].trim();
        String col = split[1].trim();
        insertData(tableName, row, colFamily, col, newValue);
        System.out.println("数据修改成功");
    }

    public static void deleteRow(String tableName, String row) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(row));
        table.delete(delete);
        table.close();
        System.out.println(tableName + " 表的 " + row + " 行删除成功");
    }
}
