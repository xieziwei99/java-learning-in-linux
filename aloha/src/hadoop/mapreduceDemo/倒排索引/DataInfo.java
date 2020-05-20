package hadoop.mapreduceDemo.倒排索引;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author xzw
 * 2020-05-20
 */
public class DataInfo implements Writable {
    private String fileName;
    private int count;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(fileName);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.fileName = dataInput.readUTF();
        this.count = dataInput.readInt();
    }

    public String getFileName() {
        return fileName;
    }

    public DataInfo setFileName(String fileName) {
        this.fileName = fileName;
        return this;
    }

    public int getCount() {
        return count;
    }

    public DataInfo setCount(int count) {
        this.count = count;
        return this;
    }

    @Override
    public String toString() {
        return fileName + "->" + count;
    }
}
