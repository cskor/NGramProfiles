package cs435.hadoop.profileThree;
import java.util.*;
import static java.util.stream.Collectors.*;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfileThreeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private Map<String, Integer> wordCount = new HashMap<>();
  protected void map(LongWritable key, Text value, Context context){

    //Article ID string
    String idBufferStr = "<====>";

    //Split the strings on the whitespace
    String valueString = value.toString();

    int startID = valueString.indexOf(idBufferStr);
    if (startID == -1)
      return;

    valueString = valueString.substring(startID + idBufferStr.length());
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

      if(wordCount.containsKey(modifiedWord))
        wordCount.put(modifiedWord, wordCount.get(modifiedWord) + 1);
      else
        wordCount.put(modifiedWord, 1);
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    //Sort all the words in the tree map by occurrence
    Map<String, Integer> sortedWordCount = wordCount.entrySet()
        .stream()
        .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));

    for(String word: sortedWordCount.keySet()){
     if(word.length() > 0 ){
       context.write(new Text(word), new IntWritable(sortedWordCount.get(word)));
     }
    }
  }

}
