import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.*;

@SuppressWarnings("deprecation")
public class Driver
{
 
    public static void main (String[] args) throws Exception
    {
        long start_time = System.currentTimeMillis();
        if (args.length != 4)
        {
            System.err.println("Please Enter the input and output parameters and also the Map and Reduce Parameters");
            System.exit(-1);
        }

        JobConf conf = new JobConf(Driver.class);
        conf.setJobName("Max temperature");
        conf.setNumMapTasks(Integer.parseInt(args[2]));
        conf.setNumReduceTasks(Integer.parseInt(args[3]));

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setMapperClass(MapperClass.class);
        conf.setReducerClass(ReducerClass.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(DoubleWritable.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);

        JobClient.runJob(conf);
        long EstimatedTime = System.currentTimeMillis() - start_time;
        System.out.println("Time taken: "+EstimatedTime);
    }
}
