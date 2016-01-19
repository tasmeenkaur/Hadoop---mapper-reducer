import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class MapperClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable>
{

    public static final int MISSING = -9999;
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
            throws IOException
    {
        String line = value.toString();
        String [] path = line.split(",");
        
	String year = String.valueOf(Double.parseDouble(path[0]));

  
      
        Double temperature;
        
        Double Tmaxtemp = Double.parseDouble(path[4].toString());
        Double Tmintemp = Double.parseDouble(path[5].toString());

        temperature = (Tmaxtemp+Tmintemp)/2;
       

        if(Tmaxtemp != MISSING && Tmintemp != MISSING) {
            output.collect(new Text(year), new DoubleWritable(temperature));
        }        }    }
