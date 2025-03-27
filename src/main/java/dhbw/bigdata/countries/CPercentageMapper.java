package dhbw.bigdata.countries;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * CPercentageMapper:
 * ----------------------
 * Berechnet Prozentänderung und gibt sie als IntWritable Key aus.
 *
 * Input:   <Country>  <2019>,<2024>
 * Output:  <percent>  <Country>,<2019>,<2024>
 */
public class CPercentageMapper extends Mapper<Object, Text, IntWritable, Text> {

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        // Eingabezeile lesen -> KEY<TAB>VALUE am Tab trennen
        String line = value.toString();
        String[] parts = line.split("\t");
        if (parts.length != 2) return;

        String stateName = parts[0].trim(); // Key (Land)
        String[] values = parts[1].split(",", -1); // Value-String am Komma trennen
        if (values.length < 2) return; // expecting two values (two numbers)

        int value2019, value2024;
        try {
            // Werte als Integer parsen
            value2019 = Integer.parseInt(values[0].trim());
            value2024 = Integer.parseInt(values[1].trim());
        } catch (NumberFormatException e) {
            return;
        }

        // Berechne prozentuale Änderung
        int percentChange = (value2019 == 0) ? 100 : ((value2024 - value2019) * 100) / value2019;

        // Values-String wieder zusammensetzen
        String valueString = stateName + "," + value2019 + "," + value2024;

        // prozentuale Änderung als Key mit neuem Value-String ausgeben
        context.write(new IntWritable(percentChange), new Text(valueString));
    }
}
