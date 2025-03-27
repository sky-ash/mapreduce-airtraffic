package dhbw.bigdata.preprocessing;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * PrepReducer:
 * ----------------------
 * Endgültige Aggregation pro Flughafen für 2019 & 2024.
 *      Ausgabe: (Eine Zeile pro Flughafen)
 *      Key         Values
 *      <ICAO>      <Country>,<2019_sum>,<2024_sum>
 */
public class PrepReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int total2019 = 0;
        int total2024 = 0;
        String stateName = "";

        // ueber alle werte (Value-Strings) des jeweiligen Keys (Flughafen-Code) iterieren
        for (Text val : values) {
            String[] parts = val.toString().split(",", -1); // Value-String am Komma trennen
            if (parts.length < 3) continue; // error handling

            stateName = parts[0].trim();        // Land des Flughafens
            String year = parts[1].trim();    // Jahr
            String flightCountStr = parts[2].trim();// Total Flights

            try {
                // Anzahl in Integer umwandeln (auch error handling)
                int flightCount = Integer.parseInt(flightCountStr);
                // dem jeweiligen Jahr aufsummieren
                if (year.equals("2019")) {
                    total2019 += flightCount;
                } else if (year.equals("2024")) {
                    total2024 += flightCount;
                }
            } catch (NumberFormatException e) {
                continue;
            }
        }

        // neuen Value-String aus Land und den aggregierten Werten für 2019 und 2024 erstellen
        String valueString = stateName + "," + total2019 + "," + total2024;
        context.write(key, new Text(valueString));
    }
}
