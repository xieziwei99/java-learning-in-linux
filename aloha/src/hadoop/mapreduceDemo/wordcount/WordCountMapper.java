package hadoop.mapreduceDemo.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author xzw
 * 2020-05-07
 */
public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Text keyout = new Text();
        IntWritable valueout = new IntWritable(1);
        String[] split = value.toString().split("\\s+");
        for (String s : split) {
            keyout.set(s);
            context.write(keyout, valueout);
        }
    }
}
