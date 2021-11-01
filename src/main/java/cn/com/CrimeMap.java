package cn.com;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author ZhangALong
 * @date 2021/9/27
 * @description
 */
public class CrimeMap extends Mapper<LongWritable, Text,Text,LongWritable> {
    LongWritable v = new LongWritable(1);
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(",");
        k.set(words[2]);
        context.write(k,v);
    }
}
