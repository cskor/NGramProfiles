package cs435.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NGramMapper extends Mapper<LongWritable, Text, LongWritable, Text > {
  protected void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
    //Split the strings on the whitespace
    String valueString = value.toString();
    String[] words = valueString.split("\\s+");

    ArrayList<String> modifiedWords = new ArrayList<>();

    long documentId = -1;
    //Loop through all the words and make the fit the unigram standards
    for (String editWord : words) {
      //find the string that contains <====>NUMBER<====> and extract number
      if (editWord.contains("<====>")) {
        editWord = editWord.replace("<====>", " ");
        String[] documentIdWords = editWord.split("\\s+");

        //Set the document ID
        documentId = Long.parseLong(documentIdWords[1]);

        modifiedWords.add(modifyString(documentIdWords[0]));
        modifiedWords.add(modifyString(documentIdWords[2]));
      } else {
        modifiedWords.add(modifyString(editWord));
      }
    }

    //Ignore newlines
    if(documentId != -1){
      for(String word: modifiedWords){
        if(word.length() > 0)
          context.write(new LongWritable(documentId), new Text(word));
      }
    }
  }

  /** All upper case must be lower case, consider only alphanumeric characters.
   *  Need to remove any and all punctuations or apostrophes */
  private String modifyString(String editWord){
    //Remove all uppercase letters
    editWord = editWord.toLowerCase();

    //Remove anything thats not alphanumeric
    editWord = editWord.replaceAll("[^A-Za-z0-9]", "");

    return editWord;
  }
}
