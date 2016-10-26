import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by nina on 10/25/16.
 */
public class NGramLibraryBuilder {
    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        int nGram;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            nGram = conf.getInt("nGram", 5);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            line = line.toLowerCase().trim();
            line = line.replaceAll("[^a-z]", " ");
            String[] words = line.split("\\s+");
            if (words.length < 2) {
                return;
            }
            StringBuilder sb;
            for (int i = 0; i < words.length - 1; i++) {
                sb = new StringBuilder();
                sb.append(words[i]);
                for (int j = 1; i + j < words.length && j < nGram; j++) {
                    sb.append(" ");
                    sb.append(words[i + j]);
                    context.write(new Text(sb.toString().trim()), new IntWritable(1));
                }
            }
        }
    }
    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum = sum + value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
