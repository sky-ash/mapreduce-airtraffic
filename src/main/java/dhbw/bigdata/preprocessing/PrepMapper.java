package dhbw.bigdata.preprocessing;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * PrepMapper:
 * ---------------------
 * - Liest CSV-Zeilen und extrahiert Flughafencode, Land, Jahr, Anzahl Flüge.
 * - Gibt nur Zeilen mit vollständigen Werten für 2019 oder 2024 weiter.
 *
 * - Ausgabe:
 *      Key         Values
 *      <ICAO>      <Country>,<Year>,<Count>
 *
 */
public class PrepMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // CSV-Zeile einlesen und in Werte aufteilen (am Komma trennen)
        String line = value.toString();
        if (line.startsWith("YEAR")) return; // Header-Zeile überspringen
        String[] parts = line.split(",", -1);
        if (parts.length < 13) return; // error handling

        // relevante Werte extrahieren
        String yearStr = parts[0].trim(); // 2019 oder 2024
        String airportCode = parts[4].trim();  // Airport-Code
        String stateName = parts[6].trim(); // Land des Flughafens
        String flightCountStr = parts[12].trim(); // Anzahl Flüge (FLT_TOT_IFR_2)

        // Nur Zeilen mit allen 4 Werten (bezweckt auch dass nur Zeilen wo FLT_TOT_IFR_2 vorhanden ist weiterverarbeitet werden)
        if (yearStr.isEmpty() || airportCode.isEmpty() || stateName.isEmpty() || flightCountStr.isEmpty()) return;

        // Nur Jahre 2019 und 2024 zulassen (auch wenn es scheint dass ohnehin nur diese Jahre vorhanden sind) -> deshalb bezwecket es dennoch error handling
        if (!yearStr.equals("2019") && !yearStr.equals("2024")) return;

        try {
            Integer.parseInt(flightCountStr); // Sicherstellen, dass es sich um eine Ganzzahl handelt
        } catch (NumberFormatException e) {
            return;
        }

        // Value-String erstellen und mit Flughafencode als Key ausgeben (Fuer jeden Flughafen: Land, Jahr, Anzahl Flüge)
        context.write(new Text(airportCode), new Text(stateName + "," + yearStr + "," + flightCountStr));
    }
}
