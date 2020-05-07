package hadoop.hdfsDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author xzw
 * 2020-04-23
 */
public class 文件操作Demo {
    private FileSystem fs = null;
    public static final Logger log = Logger.getLogger(文件操作Demo.class);

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration(), "xzw");
    }

    @Test
    public void testMkdir() throws IOException {
        boolean flag = fs.mkdirs(new Path("aa"));
        if (flag) {
            log.info("创建文件夹成功");
        } else {
            log.info("创建文件夹失败");
        }
    }

    @Test
    public void testUploadFile() throws IOException {
        FSDataOutputStream out = fs.create(new Path("aa/a.txt"));
        FileInputStream in = new FileInputStream(new File("/home/xzw/Desktop/a.txt"));
        IOUtils.copyBytes(in, out, 4096, true);
    }

    @Test
    public void testDelete() throws IOException {
        boolean flag = fs.delete(new Path("aa"), true);
        log.info(flag);
    }

    public static void main(String[] args) throws URISyntaxException, IOException {
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration());
        InputStream in = fs.open(new Path("input/a.txt"));
        FileOutputStream out = new FileOutputStream(new File("/home/xzw/Desktop/a.txt"));
        // 设置为 true, 可以把 in 和 out 关闭
        IOUtils.copyBytes(in, out, 4096, true);
    }
}
