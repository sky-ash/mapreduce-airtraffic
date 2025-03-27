package dhbw.bigdata;

import dhbw.bigdata.airports.APercentageMapper;
import dhbw.bigdata.airports.APercentageReducer;
import dhbw.bigdata.countries.CPercentageReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import dhbw.bigdata.utils.MapReduceJob;
import dhbw.bigdata.countries.CountryMapper;
import dhbw.bigdata.preprocessing.PrepMapper;
import dhbw.bigdata.preprocessing.PrepReducer;
import dhbw.bigdata.preprocessing.PrepCombiner;
import dhbw.bigdata.countries.CountryReducer;
import dhbw.bigdata.countries.CPercentageMapper;

public class Main {
    public static void main(String[] args) throws Exception {

        // Konfiguriere die MapReduce-Jobs
        MapReduceJob prep = new MapReduceJob(
                "Preprocessing", PrepMapper.class, PrepReducer.class, PrepCombiner.class,
                Text.class, Text.class, TextInputFormat.class, TextOutputFormat.class
        );
        MapReduceJob countryCalc = new MapReduceJob(
                "CountryCalc", CountryMapper.class, CountryReducer.class, null,
                Text.class, Text.class, TextInputFormat.class, TextOutputFormat.class
        );
        MapReduceJob countrySort = new MapReduceJob(
                "CountrySort", CPercentageMapper.class, CPercentageReducer.class, null,
                IntWritable.class, Text.class, TextInputFormat.class, TextOutputFormat.class
        );
        MapReduceJob airports = new MapReduceJob(
                "AirportPercentages", APercentageMapper.class, APercentageReducer.class, null,
                IntWritable.class, Text.class, TextInputFormat.class, TextOutputFormat.class
        );

        // ausführen der Jobs
        prep.run();
        countryCalc.run();
        countrySort.run();
        airports.run();

    }
}
