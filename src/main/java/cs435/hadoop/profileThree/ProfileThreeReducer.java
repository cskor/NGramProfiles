package cs435.hadoop.profileThree;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class ProfileThreeReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
  private TreeMap<String, Integer> wordCount = new TreeMap<>();

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
    Map<String, Integer> sortedWordCount = new HashMap<>();
    wordCount.entrySet()
        .stream()
        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
        .forEachOrdered(x -> sortedWordCount.put(x.getKey(), x.getValue()));

    int count = 0;
    for(String word: sortedWordCount.keySet()){
      if(count == 500)
        break;
      context.write(new Text(word + "\t" + sortedWordCount.get(word)), NullWritable.get());
      count ++;
    }
  }

}
