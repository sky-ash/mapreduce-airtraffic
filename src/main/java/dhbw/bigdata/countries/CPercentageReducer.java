package dhbw.bigdata.countries;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * CPercentageReducer:
 * ------------------------
 * Gibt Land + Werte + Prozentänderung aus, sortiert nach Prozent.
 *
 *
 * input:   <percent>       <Country>,<2019>,<2024>
 * output:  <Country>       <percent>,<2019>,<2024>
 */
public class CPercentageReducer extends Reducer<IntWritable, Text, Text, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // bekommt Prozent als Key uebergeben, damit die Werte sortiert werden
        // einfach Werte weiterreichen
        for (Text val : values) {
            String[] fields = val.toString().split(",", -1);
            if (fields.length != 3) continue;

            String stateName = fields[0].trim(); // Land (in den Values)
            String total2019 = fields[1].trim();  // Werte von 2019
            String total2024 = fields[2].trim();        // und 2024 als String uebernehmen
            String percentChange = key.toString();    // Prozentzahl aus Key (IntWritable) zu String

            // Value-String neu zusammensetzen, wieder country als key setzen und wieder ausgeben
            String valueString = percentChange + "," + total2019 + "," + total2024;
            context.write(new Text(stateName), new Text(valueString));
        }
    }
}
