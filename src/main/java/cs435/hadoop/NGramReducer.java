package cs435.hadoop;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;

public class NGramReducer extends Reducer<LongWritable, Text, Text, Text > {
  private Map<String, Integer> NGramCounts;
  private Map<String, Map<String, Integer> > DocIDCounts;
  private LinkedHashMap<String, Integer> sortedValueDocID;
  private MultipleOutputs out;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    NGramCounts = new HashMap<>();
    DocIDCounts = new HashMap<>();
    sortedValueDocID = new LinkedHashMap<>();
    out = new MultipleOutputs(context);
  }

  @Override
  protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    Map<String, Integer> wordMap;
    for(Text word: values){
      String wordKey = word.toString();

      if(!DocIDCounts.containsKey(key.toString()))
        DocIDCounts.put(key.toString(), new HashMap<>());
      else{
        wordMap = DocIDCounts.get(key.toString());
        if(!wordMap.containsKey(wordKey))
          wordMap.put(wordKey, 1);
        else
          wordMap.put(wordKey, wordMap.get(wordKey) + 1);
        DocIDCounts.put(key.toString(), wordMap);
      }

      if(!NGramCounts.containsKey(wordKey))
        NGramCounts.put(wordKey, 1);
      else
        NGramCounts.put(wordKey, NGramCounts.get(wordKey) + 1);


    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException{
     /* PROFILE ONE */
    //Print top 500 N-Grams
    TreeMap<String, Integer> sortedNGram = new TreeMap<>(NGramCounts);

    int count = 0;
    for(String word: sortedNGram.keySet()){
      if(count == 500)
        break;
      //context.write(new Text(word), new Text(""));
      out.write("one", new Text(word), NullWritable.get() );
      count ++;
    }

    /* PROFILE TWO */
    //Sort the map by key
    TreeMap<String, Map<String, Integer> > sortedDocID = new TreeMap<>(DocIDCounts);
    Map<String, Integer> wordMap;

    for(String docID: sortedDocID.keySet()){
      wordMap = sortedDocID.get(docID);
      sortedValueDocID.clear();

      //Sort the DocID map by key
      wordMap.entrySet()
          .stream()
          .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
          .forEachOrdered(x -> sortedValueDocID.put(x.getKey(), x.getValue()));

      count = 0;
      for(String word: sortedValueDocID.keySet()){
        if(count == 500)
          break;
        //context.write(new Text(docID + "\t" + word + "\t" + sortedValueDocID.get(word)), new Text(""));
        out.write("two", new Text(docID + "\t" + word + "\t" + sortedValueDocID.get(word)), NullWritable.get());
        count ++;
      }
    }

    /* PROFILE THREE */
    //Sort the N-Gram map by key
    LinkedHashMap<String, Integer> sortedValueNGram = new LinkedHashMap<>();
    NGramCounts.entrySet()
        .stream()
        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
        .forEachOrdered(x -> sortedValueNGram.put(x.getKey(), x.getValue()));

    count = 0;
    for(String word: sortedValueNGram.keySet()){
      if(count == 500)
        break;
      //context.write(new Text(word + "\t"), new Text(sortedValueNGram.get(word).toString()));
      out.write("three", new Text(word + "\t" + sortedValueNGram.get(word).toString()), NullWritable.get());
      count ++;
    }

    out.close();
  }
}
