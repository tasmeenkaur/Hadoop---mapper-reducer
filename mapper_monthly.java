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

  
      
 if(Month== 1)
        {
            year=year.concat("January");
        }
        if(Month== 2)
        {
            year=year.concat("Feb");
        }
        if(Month== 3)
        {
            year=year.concat("March");
        }
        if(Month== 4)
        {
            year=year.concat("April");
        }
        if(Month== 5)
        {
            year=year.concat("May");
        }
        if(Month== 6)
        {
            year=year.concat("June");
        }
        if(Month== 7)
        {
            year=year.concat("July");
        }
        if(Month== 8)
        {
            year=year.concat("August");
        }
        if(Month== 9)
        {
            year=year.concat("Sept");
        }
        if(Month== 10)
        {
            year=year.concat("Oct");
        }
        if(Month== 11)
        {
            year=year.concat("Nov");
        }
        if(Month== 12)
        {
            year=year.concat("Dec");
        }
        Double Tmaxtemp = Double.parseDouble(path[4].toString());
        Double Tmintemp = Double.parseDouble(path[5].toString());

        temperature = (Tmaxtemp+Tmintemp)/2;
       

        if(Tmaxtemp != MISSING && Tmintemp != MISSING) {
            output.collect(new Text(year), new DoubleWritable(temperature));
        }        }    }

		