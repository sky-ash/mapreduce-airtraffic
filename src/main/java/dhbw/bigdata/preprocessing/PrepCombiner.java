package dhbw.bigdata.preprocessing;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * PrepCombiner:
 * -----------------------
 * Aggregiert lokale Zwischensummen je Flughafen & Jahr.
 */
public class PrepCombiner extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Map<String, Integer> yearSums = new HashMap<>();
        String stateName = "";

        // ueber alle Value-Strings des jeweiligen Keys (Flughafen-Code) iterieren
        for (Text val : values) {
            String[] parts = val.toString().split(",", -1); // Value-String am Komma trennen
            if (parts.length < 3) continue; // error handling

            // Werte extrahieren
            stateName = parts[0].trim();          // Land des Flughafens
            String year = parts[1].trim();      // Jahr
            String flightCountStr = parts[2].trim();  // Anzahl

            try {
                int flightCount = Integer.parseInt(flightCountStr); // Anzahl in Integer umwandeln (auch error handling)
                yearSums.put(year, yearSums.getOrDefault(year, 0) + flightCount); // Wert auf das jeweilige Jahr aufsummieren (getOrDefault als error handling falls Jahr nicht existiert)
            } catch (NumberFormatException e) {
                continue;
            }
        }

        // Aggregierte Werte ihrem jeweiligen Key in der Map (Jahreszahl, entweder 2019 oder 2024) zuweisen
        for (Map.Entry<String, Integer> entry : yearSums.entrySet()) {
            // neuen Value-String aus Land (des Flughafens) und Jahr + Summe aus der Map erstellen
            String valueString = stateName + "," + entry.getKey() + "," + entry.getValue();
            context.write(key, new Text(valueString)); // Key (Flughafen Code) und Value-String
        }
    }
}
