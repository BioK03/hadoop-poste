import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PosteReduce extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> i = values.iterator();
        
        String occ = "";
        int j = 0;
        while (i.hasNext()){
        	Text temp = i.next();
        	occ += "("+temp+"),";
        	j++;
        }
        
        occ = occ.subSequence(0, occ.length()-1).toString();
        
        key = new Text(key+" ("+j+")");
        	
        context.write(key, new Text("Occurences : "+occ));
    }
}
