package dhbw.bigdata.airports;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;

/**
 * APercentageMapper:
 * -----------------------
 * Liest Zeilen aus Preprocessing, berechnet prozentuale Änderung der Flugbewegungen.
 * 
 * Input:   <ICAO>     <Country>,<2019>,<2024>
 * Output:  <percent>  <ICAO>,<Country>,<2019>,<2024>
 */
public class APercentageMapper extends Mapper<Object, Text, IntWritable, Text> {

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        // Eingabezeile lesen (aus dem Output von Preprocessing) -> KEY<TAB>VALUE am Tab trennen
        String line = value.toString();
        String[] parts = line.split("\t");
        if (parts.length != 2) return; // error handling

        String airportCode = parts[0].trim(); // Key (Airport Code)
        String[] fields = parts[1].split(",", -1); // Value-String am Komma trennen
        if (fields.length < 3) return;

        String stateName = fields[0].trim(); // Land des Flughafens
        int total2019, total2024;
        try {
            // Sicherstellen, dass Werte als Integer geparsed werden können
            total2019 = Integer.parseInt(fields[1].trim());
            total2024 = Integer.parseInt(fields[2].trim());
        } catch (NumberFormatException e) {
            return;
        }

        // Berechne prozentuale Änderung
        int percentChange = (total2019 == 0) ? 100 : ((total2024 - total2019) * 100) / total2019;

        String valueString = airportCode + "," + stateName + "," + total2019 + "," + total2024; // neuen Value-String erstellen
        context.write(new IntWritable(percentChange), new Text(valueString)); // prozentuale Änderung als Key
    }
}
