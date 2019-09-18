package cs435.hadoop.profileOne;

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProfileOneReducer extends Reducer<Text, NullWritable, Text, NullWritable > {
  private TreeMap<String, Integer> top500 = new TreeMap<>();

  @Override
  protected void reduce(Text key, Iterable<NullWritable> values, Context context) {
    top500.put(key.toString(), 1);
    if(top500.size() > 500)
      top500.remove(top500.lastKey());
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    for(String word: top500.keySet())
      context.write(new Text(word), NullWritable.get());
  }
}
