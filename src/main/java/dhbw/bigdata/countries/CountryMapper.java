package dhbw.bigdata.countries;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * CountryMapper:
 * -----------------------
 * Liest Preprocessing-Ausgabe, extrahiert Land und Summen zur weiteren Aggregation.
 * 
 * Input:   <ICAO>     <Country>,<2019>,<2024>
 * Output:  <Country>  <2019>,<2024>
 */
public class CountryMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        // Eingabezeile lesen (aus dem Output von Preprocessing) -> KEY<TAB>VALUE am Tab trennen
        String line = value.toString();
        String[] parts = line.split("\t");
        if (parts.length != 2) return; // error handling

        String[] fields = parts[1].split(",", -1); // Value-String am Komma trennen
        if (fields.length < 3) return; // error handling

        String stateName = fields[0].trim(); // Land des Flughafens
        String val2019 = fields[1].trim(); // Wert von 2019
        String val2024 = fields[2].trim(); // Wert von 2024

        // Value-String erstellen und mit Country als Key ausgeben 
        context.write(new Text(stateName), new Text(val2019 + "," + val2024));
    }
}
