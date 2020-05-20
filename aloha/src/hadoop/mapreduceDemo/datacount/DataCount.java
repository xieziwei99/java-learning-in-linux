package hadoop.mapreduceDemo.datacount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author xzw
 * 2020-05-20
 *
 */
public class DataCount {

    public static class DataCountMapper extends Mapper<LongWritable, Text, Text, DataInfo> {

        private final Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split("\t");
            String tel = split[1];
            long up = Long.parseLong(split[8]);
            long down = Long.parseLong(split[9]);
            DataInfo info = new DataInfo(tel, up, down);
            k.set(tel);
            context.write(k, info);
        }
    }

    public static class DataCountReducer extends Reducer<Text, DataInfo, Text, DataInfo> {
        @Override
        protected void reduce(Text key, Iterable<DataInfo> values, Context context) throws IOException, InterruptedException {
            long upSum = 0;
            long downSum = 0;
            for(DataInfo d : values){
                upSum += d.getUpPayLoad();
                downSum += d.getDownPayLoad();
            }
            DataInfo dataInfo = new DataInfo("",upSum,downSum);

            context.write(key, dataInfo);
        }
    }

    public static class DataCountPartitioner extends Partitioner<Text, DataInfo> {
        private static final Map<String,Integer> provider = new HashMap<>();
        static{
            provider.put("138", 1);
            provider.put("139", 1);
            provider.put("152", 2);
            provider.put("153", 2);
            provider.put("182", 3);
            provider.put("183", 3);
        }
        @Override
        public int getPartition(Text text, DataInfo dataInfo, int i) {
            String telSub = text.toString().substring(0,3);
            Integer count = provider.get(telSub);
            if (count == null) {
                count = 0;
            }
            return count;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // hadoop jar 运行时的参数
        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainingArgs.length != 3) {
            System.err.println("输入目录 + 输出目录 + reducer个数");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(DataCount.class);
        job.setMapperClass(DataCountMapper.class);
        job.setReducerClass(DataCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataInfo.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DataInfo.class);
        FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));
        job.setPartitionerClass(DataCountPartitioner.class);
        job.setNumReduceTasks(Integer.parseInt(remainingArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
