package solutions;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FrequencyReducer extends Reducer<Object, NullWritable, Object, IntWritable> {
    private final IntWritable count = new IntWritable(0);
    @Override
    protected void reduce(Object key, Iterable<NullWritable> values, Context context) throws InterruptedException, IOException {
        int c = 0;
        Iterator iter = values.iterator();
        while(iter.hasNext()) {
            c++;
            iter.next();
        }
        count.set(c);
        context.write(key, count);
    }
}

