package cs435.hadoop.profileOne;

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ProfileOneMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
  private TreeMap<String, Integer> top500 = new TreeMap<>();

  protected void map(LongWritable key, Text value, Context context) {
    //Split the strings on the whitespace
    String valueString = value.toString();

    String idBufferStr = "<====>";
    int startID = valueString.indexOf(idBufferStr);

    //These are the empty strings
    if(startID == -1)
      return;

    valueString = valueString.substring(startID + idBufferStr.length() );

    String[] words = valueString.split("\\s+");

    String modifiedWord;
    //Loop through all the words and make the fit the unigram standards
    for (String editWord : words) {
      //find the string that contains <====>NUMBER<====> and extract number
      if (editWord.contains(idBufferStr)) {
        editWord = editWord.replace(idBufferStr, " ");
        String[] documentIdWords = editWord.split("\\s+");

        modifiedWord = documentIdWords[1].toLowerCase().replaceAll("[^A-Za-z0-9]", "");
      } else {
        modifiedWord = editWord.toLowerCase().replaceAll("[^A-Za-z0-9]", "");
      }

      top500.put(modifiedWord, 1);
      if(top500.size() > 500)
        top500.remove(top500.lastKey());
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    for(String word: top500.keySet()){
      context.write(new Text(word), NullWritable.get());
    }
  }
}
