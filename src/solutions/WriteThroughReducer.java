package solutions;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WriteThroughReducer extends Reducer<Object, NullWritable, Object, NullWritable> {
    @Override
    protected void reduce(Object key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}

