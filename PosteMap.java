import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PosteMap extends Mapper<Object, Text, Text, Text> {
	
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // Un StringTokenizer va nous permettre de parcourir chacun des mots de la ligne qui est passée
        // à notre opération MAP.
        StringTokenizer tok = new StringTokenizer(value.toString(), "\n");
        while (tok.hasMoreTokens()) {
            Text word = new Text(tok.nextToken());
            // On renvoie notre couple (clef;valeur): le mot courant suivi de la valeur 1 (définie dans la constante ONE).
            context.write(extractData(word, 6), extractData(word, 8));
        }
    }
	
	protected Text extractData(Text input, int column){
		String sInput = input.toString();
		String[] columns = sInput.split(";");
		return new Text(columns[column]);
	}
	
	
}
