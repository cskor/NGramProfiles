package cs435.hadoop.profileTwo;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfileTwoMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    //Article ID string
    String idBufferStr = "<====>";

    //Split the strings on the whitespace
    String valueString = value.toString();

    int startID = valueString.indexOf(idBufferStr);
    if (startID == -1)
      return;

    valueString = valueString.substring(startID + idBufferStr.length());

    String[] words = valueString.split("\\s+");

    long documentId = -1;
    ArrayList<String> modifiedWords = new ArrayList<>();

    //Loop through all the words and make the fit the unigram standards
    for (String editWord : words) {
      //find the string that contains <====>NUMBER<====> and extract number
      if (editWord.contains(idBufferStr)) {
        editWord = editWord.replace(idBufferStr, " ");
        String[] documentIdWords = editWord.split("\\s+");

        //Set the document ID
        documentId = Long.parseLong(documentIdWords[0]);

        modifiedWords.add(documentIdWords[1].toLowerCase().replaceAll("[^A-Za-z0-9]", ""));
      } else {
        modifiedWords.add(editWord.toLowerCase().replaceAll("[^A-Za-z0-9]", ""));
      }
    }

    //Ignore newlines
    if (documentId != -1) {
      for (String word : modifiedWords) {
        if (word.length() > 0)
          context.write(new LongWritable(documentId), new Text(word));
      }
    }
  }
}
