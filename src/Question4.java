import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question4 {

	public static class Map extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String tmp = value.toString().toLowerCase();
			if (tmp.contains("stanford")) {
				context.write(new Text(tmp.split("\\^")[0]),
						NullWritable.get());
			}

		}

	}

	public static class JoinMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private HashMap<String, Integer> businessIDs = new HashMap<>();

		public void setup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			Path myfilepath = new Path("hdfs://cshadoop1"
					+ conf.get("Business"));
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(myfilepath);
			BufferedReader brObject = null;
			String line = null;
			int count = 0;

			for (FileStatus status : fss) {
				Path pt = status.getPath();

				brObject = new BufferedReader(
						new InputStreamReader(
								fs.open(pt)));
				while ((line = brObject.readLine()) != null) {
					businessIDs.put(line.trim()
							.toLowerCase(), count);
					count++;
				}

			}
			brObject.close();
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String lineRead = value.toString();
			String[] lineArr = lineRead.split("\\^");
			boolean found = false;
			String rating = null, userID = null;

			if (businessIDs.containsKey(lineArr[2].toLowerCase())) {
				found = true;
				userID = lineArr[1];
				rating = lineArr[3];
			}

			if (found)
				context.write(new Text(userID),
						new Text(rating));
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		// get all args
		if (otherArgs.length < 2) {
			System.err.println("Usage: Question4 <reviews.csv> <business.csv> <businessTmp>  <output>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Question4");
		job.setNumReduceTasks(0);

		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setJarByClass(Question4.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		job.waitForCompletion(true);

		Configuration confMapJoin = new Configuration();
		confMapJoin.set("Business", otherArgs[2]);
		Job jobMapJoin = Job.getInstance(confMapJoin, "MapSideJoin");

		jobMapJoin.setJobName("MapSideJoin");
		jobMapJoin.setNumReduceTasks(0);

		jobMapJoin.setJarByClass(Question4.class);
		jobMapJoin.setMapperClass(JoinMapper.class);
		jobMapJoin.setMapOutputKeyClass(Text.class);
		jobMapJoin.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(jobMapJoin, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(jobMapJoin, new Path(
				otherArgs[3]));

		if (jobMapJoin.waitForCompletion(true)) {
			System.exit(2);
		}

	}

}
