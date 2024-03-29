package cs435.hadoop.profileThree;

import java.io.IOException;
import java.util.*;
import static java.util.stream.Collectors.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class ProfileThreeReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
  private Map<String, Integer> wordCount = new HashMap<>();

  @Override
  protected void reduce(Text key, Iterable<IntWritable> values, Context context){
    int total = 0;
    for(IntWritable value: values){
      total += value.get();
    }

    wordCount.put(key.toString(), total);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    //Sort all the words in the tree map by occurrence
    Map<String, Integer> sortedWordCount = wordCount.entrySet()
        .stream()
        .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
        .collect( toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));
    int count = 0;
    for(String word: sortedWordCount.keySet()){
      if(count == 500)
        break;
      context.write(new Text(word + "\t" + sortedWordCount.get(word)), NullWritable.get());
      count ++;
    }
  }

}
