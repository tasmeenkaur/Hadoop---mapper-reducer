import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


@SuppressWarnings("deprecation")
public class ReducerClass extends MapReduceBase
        implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterator<DoubleWritable> values,
                       OutputCollector<Text, DoubleWritable> output, Reporter reporter)
            throws IOException {

        double max_temp = 0;
        int count = 0;
        while (values.hasNext()) {
            max_temp += values.next().get();
            count+=1;
        }
        output.collect(key, new DoubleWritable(max_temp/count));
    }
}
