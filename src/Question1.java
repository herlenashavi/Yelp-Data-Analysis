import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question1 {

	public static class Map extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		private Text businessId = new Text();
		String location;

		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			location = conf.get("Location");
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] lineSplit = line.split("\\^");

			String tmp = lineSplit[1].toLowerCase();
			if (tmp.contains(location.trim())) {
				businessId.set(lineSplit[0]);
				context.write(businessId, NullWritable.get());
			}

		}

	}

	public static class Reduce extends
			Reducer<Text, NullWritable, Text, NullWritable> {

		public void reduce(Text key, NullWritable values,
				Context context) throws IOException,
				InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		// get all args
		if (otherArgs.length < 3) {
			System.err.println("Usage: Question1 <in> <out> <Location Name>");
			System.exit(2);
		}
		StringBuilder businessLoc = new StringBuilder();

		for (int i = 2; i < otherArgs.length; i++) {
			businessLoc.append(otherArgs[i] + " ");
		}
		conf.set("Location", businessLoc.toString().toLowerCase());

		Job job = Job.getInstance(conf, "Question1");
		job.setJarByClass(Question1.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// set output key type
		job.setOutputKeyClass(Text.class);

		// set output value type
		job.setOutputValueClass(NullWritable.class);

		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}