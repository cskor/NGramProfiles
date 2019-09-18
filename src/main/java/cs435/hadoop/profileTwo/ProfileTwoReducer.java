package cs435.hadoop.profileTwo;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProfileTwoReducer extends Reducer<LongWritable, Text, Text, NullWritable> {
  private TreeMap<String, Integer> wordCount = new TreeMap<>();
  private TreeMap<String, Integer> sortedWordCount = new TreeMap<>();
  @Override
  protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    wordCount.clear();

    //Add word to tree map with value being occurrences
    for(Text value: values){
      String word = value.toString();

      if(!wordCount.containsKey(word))
        wordCount.put(word, 1);
      else
        wordCount.put(word, wordCount.get(word) + 1);
    }

    //Sort the tree map by value
    wordCount.entrySet()
        .stream()
        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
        .forEachOrdered(x -> sortedWordCount.put(x.getKey(), x.getValue()));

    //Print the top 500
    int count = 0;
    for(String word: sortedWordCount.keySet()){
      if(count == 500)
        break;
      context.write(new Text(key.toString() + "\t" + word + "\t" + sortedWordCount.get(word) ), NullWritable.get());
      count ++;
    }
  }
}
