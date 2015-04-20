package count;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MeanTestDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// Use programArgs array to retrieve program arguments.
		String[] programArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		Job job = new Job(conf);
		job.setJarByClass(MeanTestDriver.class);
		job.setMapperClass(MeanTestMapper.class);
		job.setReducerClass(MeanTestReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);


		FileInputFormat.addInputPath(job, new Path(programArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(programArgs[1]));


		// Submit the job and wait for it to finish.
		job.waitForCompletion(true);
		// Submit and return immediately: 
		// job.submit();
	}

}
