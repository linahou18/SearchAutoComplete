import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by nina on 10/26/16.
 */
public class LanguageModel {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        int threshold;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            threshold = conf.getInt("threshold", 20);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value == null || (value.toString().trim()).length() == 0) {
                return;
            }
            String line = value.toString().trim();
            String[] wordsPlusCount = line.split("\t");
            if (wordsPlusCount.length < 2) {
                return;
            }
            int count = Integer.valueOf(wordsPlusCount[1]);
            String[] words = wordsPlusCount[0].split("\\s+"); // all kinds of spaces

            if (count < threshold) {
                return;
            }

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < words.length - 1; i++) {
                sb.append(words[i]).append(" ");
            }
            String outputKey = sb.toString().trim();
            String outputValue = words[words.length - 1];
            if(!((outputKey == null) || (outputKey.length() <1))) {
                context.write(new Text(outputKey), new Text(outputValue + "=" + count));
            }
        }
    }
    public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

        int n;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            n = conf.getInt("n", 5);
        }

        private Comparator<Pair> pairComparator = new Comparator<Pair>() {
            public int compare(Pair left, Pair right) {
                if (left.value != right.value) {
                    return left.value - right.value;
                } else {
                    return right.key.compareTo(left.key);
                }
            }
        };

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //this is, <girl = 50, boy = 60>
            HashMap<String, Integer> map = new HashMap<String, Integer>();
            for (Text value : values) {
                String curValue = value.toString().trim();
                String word = curValue.split("=")[0].trim();
                int count = Integer.parseInt(curValue.split("=")[1].trim());
                map.put(word, count);
            }
            Queue<Pair> queue = new PriorityQueue<Pair>(n, pairComparator);
            for (String word : map.keySet()) {
                Pair cur = new Pair(word, map.get(word));
                Pair peek = queue.peek();
                if (queue.size() < n) {
                    queue.offer(cur);
                } else {
                    if (pairComparator.compare(peek, cur) < 0) {
                        queue.poll();
                        queue.offer(cur);
                    }
                }
            }
            while (!queue.isEmpty()) {
                Pair curP = queue.poll();
                String curWord = curP.key;
                int wordCount = curP.value;
                context.write(new DBOutputWritable(key.toString(), curWord, wordCount),NullWritable.get());
            }
        }
    }

}
class Pair {
    String key;
    int value;
    Pair(String key, int value) {
        this.key = key;
        this.value = value;
    }
}
