package cs435.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NGramJob {
  public static void main(String[] args) {
    try {
      //Setup for the first job
      Configuration conf = new Configuration();

      //Setup for jar of class
      Job job = Job.getInstance(conf, "N-Gram Profiles");
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
      job.setMapOutputKeyClass(LongWritable.class);
      job.setMapOutputValueClass(Text.class);

      //Set the outputs
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      // path to input in HDFS
      FileInputFormat.addInputPath(job, new Path(args[0]));
      // path to output in HDFS
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      MultipleOutputs.addNamedOutput(job, "one", TextOutputFormat.class, Text.class, Text.class);
      MultipleOutputs.addNamedOutput(job, "two", TextOutputFormat.class, Text.class, Text.class);
      MultipleOutputs.addNamedOutput(job, "three", TextOutputFormat.class, Text.class, Text.class);
      LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

      // Block until the job is completed.
      System.exit(job.waitForCompletion(true) ? 0 : 1);

    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      System.err.println(e.getMessage());
    }

  }
}
