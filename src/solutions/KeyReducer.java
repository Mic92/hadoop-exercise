package solutions;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KeyReducer extends Reducer<Object, Object, Object, NullWritable> {
    @Override
    protected void reduce(Object key, Iterable<Object> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}

