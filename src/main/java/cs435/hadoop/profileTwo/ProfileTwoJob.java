package cs435.hadoop.profileTwo;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProfileTwoJob {
  public static void main(String[] args) {
    try {
      //Setup for the first job
      Configuration conf = new Configuration();

      //Setup for jar of class
      Job job = Job.getInstance(conf, "N-Gram Profile Two");
      job.setJarByClass(cs435.hadoop.profileTwo.ProfileTwoJob.class);

      job.setMapperClass(ProfileTwoMapper.class);
      job.setReducerClass(ProfileTwoReducer.class);
      job.setNumReduceTasks(10);

      // Outputs from the Mapper.
      job.setMapOutputKeyClass(LongWritable.class);
      job.setMapOutputValueClass(Text.class);

      //Set the outputs
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(NullWritable.class);

      // path to input in HDFS
      FileInputFormat.addInputPath(job, new Path(args[0]));
      // path to output in HDFS
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      // Block until the job is completed.
      System.exit(job.waitForCompletion(true) ? 0 : 1);

    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      System.err.println(e.getMessage());
    }

  }
}
