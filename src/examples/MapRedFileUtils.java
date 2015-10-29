package examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MapRedFileUtils
{
    /**
     * Deletes a (result) directory.
     */
    public static void deleteDir(String path)
    {
        boolean result = true;
        try
        {
            Configuration conf = new Configuration();
            
            FileSystem fs = FileSystem.get(conf);

            if (fs.exists(new Path(path)))
            result = fs.delete(new Path(path), true);
            if (!result || fs.exists(new Path(path)))
            System.err.println("Deleted directory does still exist: "  + path);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
