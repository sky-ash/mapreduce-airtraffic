package dhbw.bigdata.airports;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;

/**
 * APercentageReducer:
 * ------------------------
 * Gibt sortierte Flughafen-Auswertungen aus.
 * 
 * Input:   <percent>  <ICAO>,<Country>,<2019>,<2024>
 * Output:  <ICAO>     <Country>,<2019>,<2024>,<percent>
 */
public class APercentageReducer extends Reducer<IntWritable, Text, Text, Text> {

    @Override
    protected void reduce(IntWritable percentKey, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // ueber alle Value-Strings mit dem gleichen Key (Prozentzahl) iterieren
        for (Text val : values) {
            String[] fields = val.toString().split(",", -1); // am Komma trennen
            if (fields.length != 4) continue; // error handling

            String airportCode = fields[0].trim();     // Flughafen-Code
            String stateName = fields[1].trim();  // Land des Flughafens
            String total2019 = fields[2].trim();  // Wert von 2019
            String total2024 = fields[3].trim();  // Wert von 2024
            String percentChange = percentKey.toString(); // Prozentzahl

            // Felder neu zuweisen: Prozentuale Änderung wieder als Value, ICAO wieder als Key
            String valueString = percentChange + ',' + total2019 + "," + total2024 + "," + stateName;
            context.write(new Text(airportCode), new Text(valueString)); // Finale Ausgabe
        }
    }
}
