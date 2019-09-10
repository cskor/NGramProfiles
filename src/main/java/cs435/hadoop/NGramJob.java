package cs435.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NGramJob {
  public static void main(String[] args) {
    try {
      //Setup for the first job
      Configuration conf = new Configuration();

      //Setup for jar of class
      Job job = Job.getInstance(conf, "Million Song Dataset");
      job.setJarByClass(NGramJob.class);

      //Set input/output paths
      job.setMapperClass(NGramMapper.class);
      //MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MetaDataMapper.class);
      //MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AnalysisDataMapper.class);
      //FileOutputFormat.setOutputPath(job, new Path(args[2]));

      //Set the mapper, reducer, and combiner
      //job.setCombinerClass(SongReducer.class);
      job.setReducerClass(NGramReducer.class);
      //job.setNumReduceTasks(1);

      //Set the shuffle phase comparator
      //job.setSortComparatorClass(IntComparator.class);

      // Outputs from the Mapper.
      //job.setMapOutputKeyClass(Text.class);
      //job.setMapOutputValueClass(Text.class);

      //Set the outputs
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      // Block until the job is completed.
      System.exit(job.waitForCompletion(true) ? 0 : 1);

    } catch (IOException e) {
      System.err.println(e.getMessage());
    } catch (InterruptedException e) {
      System.err.println(e.getMessage());
    } catch (ClassNotFoundException e) {
      System.err.println(e.getMessage());
    }

  }
}
