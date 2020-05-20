package hadoop.mapreduceDemo.倒排索引;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author xzw
 * 2020-05-20
 */
public class Main {

    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, DataInfo> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            Text keyout = new Text();
            DataInfo dataInfo = new DataInfo().setCount(1).setFileName(filename);
            String[] split = value.toString().split("\\s+");
            for (String s : split) {
                keyout.set(s);
                context.write(keyout, dataInfo);
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, DataInfo, Text, List<DataInfo>> {
        @Override
        protected void reduce(Text key, Iterable<DataInfo> values, Context context) throws IOException, InterruptedException {
            List<DataInfo> dataInfoList = new ArrayList<>();

            Map<String, Integer> map = new HashMap<>();
            for (DataInfo info : values) {
                map.merge(info.getFileName(), info.getCount(), Integer::sum);
            }
            map.forEach((s, integer) -> dataInfoList.add(new DataInfo().setFileName(s).setCount(integer)));
            dataInfoList.sort((o1, o2) -> o2.getCount() - o1.getCount());
            context.write(key, dataInfoList);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainingArgs.length != 2) {
            System.err.println("参数： 输入目录 + 输出目录");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "倒排索引");
        job.setJarByClass(Main.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataInfo.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DataInfo[].class);
        FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
