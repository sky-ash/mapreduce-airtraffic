package dhbw.bigdata.countries;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * CountryReducer:
 * ---------------------------
 * Aggregiert die Flugbewegungen je Land (Summe 2019 & 2024).
 *
 * Input:   <Country>  <2019>,<2024>
 * Output:  <Country>  <total2019>,<total2024>
 */
public class CountryReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text country, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int total2019 = 0;
        int total2024 = 0;

        // ueber alle Value-Strings mit dem gleichen Key (Land) iterieren
        for (Text val : values) {
            String[] parts = val.toString().split(",", -1);
            if (parts.length < 2) continue;

            String val2019 = parts[0].trim();
            String val2024 = parts[1].trim();

            try {
                int value2019 = Integer.parseInt(val2019);
                int value2024 = Integer.parseInt(val2024);
                total2019 += value2019;
                total2024 += value2024;
            } catch (NumberFormatException e) {
                continue;
            }
        }

        // for now just pass it into the value String
        String valueString = total2019 + "," + total2024;
        context.write(country, new Text(valueString));
    }
}
