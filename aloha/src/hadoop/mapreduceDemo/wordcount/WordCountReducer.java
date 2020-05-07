package hadoop.mapreduceDemo.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author xzw
 * 2020-05-07
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        AtomicInteger sum = new AtomicInteger();
        values.forEach(intWritable -> sum.addAndGet(intWritable.get()));
        context.write(key, new IntWritable(sum.get()));
    }
}
