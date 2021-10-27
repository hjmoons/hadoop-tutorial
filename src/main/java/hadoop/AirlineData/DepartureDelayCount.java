package hadoop.AirlineData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DepartureDelayCount {
    public static void main(String[] args) throws Exception {
        if (args.length != 2){ // 입출력 데이터 확인
            System.err.println("Usage: DepartureDelayCount <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = new Job(conf, "DepartureDelayCount");

        // 입출력 경로 설정
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 입출력 포맷 설정
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Job, Mapper, Reducer 클래스 설정
        job.setJarByClass(DepartureDelayCount.class);
        job.setMapperClass(DepartureDelayCountMapper.class);
        job.setReducerClass(DelayCountReducer.class);

        // 출력 Key, Value 유형 설정
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }
}