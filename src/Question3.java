import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question3 {

	public static class Map extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		private Text businessId = new Text();
		private DoubleWritable rating = new DoubleWritable();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] lineSplit = line.split("\\^");

			businessId.set(lineSplit[2]);
			rating.set(Double.parseDouble(lineSplit[3].trim()));
			context.write(businessId, rating);

		}
	}

	public static class Reduce extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		private DoubleWritable average = new DoubleWritable();

		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException,
				InterruptedException {

			Double sum = 0.0;
			Double count = 0.0;

			for (DoubleWritable intValue : values) {
				sum += intValue.get();
				count++;
			}
			average.set(sum / count);
			context.write(key, average);

		}
	}

	public static class MapJob2 extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(new IntWritable(1), value);
		}
	}

	public static class ReduceJob2 extends
			Reducer<IntWritable, Text, Text, DoubleWritable> {

		private DoubleWritable average = new DoubleWritable();
		private Text businessId = new Text();
		private static HashMap<String, Double> businessMap = new HashMap<String, Double>();
		private static int topTen = 0;

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException,
				InterruptedException {

			for (Text value : values) {
				String[] line = value.toString()
						.split("[\\s]+");
				businessMap.put(line[0],
						Double.parseDouble(line[1]));
			}

			TreeMap<String, Double> sortedMap = sortByValue(businessMap);
			for (java.util.Map.Entry<String, Double> entry : sortedMap
					.entrySet()) {
				businessId.set(entry.getKey());
				average.set(entry.getValue());
				context.write(businessId, average);
				topTen++;
				if (topTen == 10) {
					break;
				}
			}

		}

	}

	private static TreeMap<String, Double> sortByValue(
			HashMap<String, Double> map) {
		ValueComparator vc = new ValueComparator(map);
		TreeMap<String, Double> sortedMap = new TreeMap<String, Double>(
				vc);
		sortedMap.putAll(map);
		return sortedMap;
	}

	static class ValueComparator implements Comparator<String> {

		java.util.Map<String, Double> map;

		public ValueComparator(java.util.Map<String, Double> base) {
			this.map = base;
		}

		public int compare(String a, String b) {
			if (map.get(a) >= map.get(b)) {
				return -1;
			} else {
				return 1;
			}
		}
	}

	public static class MapBusinesses extends
			Mapper<LongWritable, Text, Text, Text> {
		private Text businessId = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] lineSplit = line.split("\\^");
			String businessDetails = "";
			businessId.set(lineSplit[0]);
			for (int i = 1; i < lineSplit.length; i++) {
				businessDetails += lineSplit[i] + " ";
			}
			businessDetails += "RATING";
			context.write(businessId, new Text(businessDetails));

		}
	}

	public static class MapReviews extends
			Mapper<LongWritable, Text, Text, Text> {
		private Text businessId = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] lineSplit = line.split("\\t");
			businessId.set(lineSplit[0]);

			String reviewDetails = "* " + lineSplit[1];
			context.write(businessId, new Text(reviewDetails));

		}
	}

	public static class ReduceJoinJob extends
			Reducer<Text, Text, Text, Text> {
		private Text businessId = new Text();
		private Text businessDetails = new Text();

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException,
				InterruptedException {
			String rating = null;
			String bizDetails = null;
			boolean ratingPresent = false, detailsPresent = false;
			for (Text val : values) {

				if (val.toString().trim().contains("*")) {
					rating = val.toString().replace("* ",
							"");
					ratingPresent = true;
				}
				if (val.toString().trim().contains("RATING")) {
					bizDetails = val.toString().replace(
							"RATING", "");
					detailsPresent = true;
				}

			}
			if (ratingPresent && detailsPresent) {
				businessId.set(key);
				businessDetails.set(bizDetails + " " + rating);
				context.write(businessId, businessDetails);
			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		// get all args
		if (otherArgs.length < 2) {
			System.err.println("Usage: Question3 <reviews.csv> <business.csv> <reviewsTmp> <TopTen> <output>");
			System.exit(2);
		}

		// create a job with name "Question3"
		Job job = Job.getInstance(conf, "Question3");
		job.setJarByClass(Question3.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// uncomment the following line to add the Combiner
		// job.setCombinerClass(Reduce.class);

		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(DoubleWritable.class);
		// set the HDFS path of the input data

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		Job job2 = null;
		// Wait till job completion
		if (job.waitForCompletion(true)) {
			Configuration conf2 = new Configuration();
			job2 = Job.getInstance(conf2, "CalculatingTopTen");
			job2.setMapperClass(MapJob2.class);
			job2.setReducerClass(ReduceJob2.class);

			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setMapOutputValueClass(Text.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(DoubleWritable.class);

			job2.setJarByClass(Question3.class);

			FileInputFormat.addInputPath(job2, new Path(
					otherArgs[2]));
			FileOutputFormat.setOutputPath(job2, new Path(
					otherArgs[3]));

		}

		Configuration confJoin;
		Job jobReduceJoin;

		if (job2.waitForCompletion(true)) {
			confJoin = new Configuration();
			jobReduceJoin = Job.getInstance(confJoin,
					"ReduceSideJoin");
			jobReduceJoin.setJarByClass(Question3.class);
			jobReduceJoin.setReducerClass(ReduceJoinJob.class);

			MultipleInputs.addInputPath(jobReduceJoin, new Path(
					otherArgs[1]), TextInputFormat.class,
					MapBusinesses.class);
			MultipleInputs.addInputPath(jobReduceJoin, new Path(
					otherArgs[3]), TextInputFormat.class,
					MapReviews.class);

			jobReduceJoin.setMapOutputKeyClass(Text.class);
			jobReduceJoin.setMapOutputValueClass(Text.class);
			jobReduceJoin.setOutputKeyClass(Text.class);
			jobReduceJoin.setOutputValueClass(NullWritable.class);
			jobReduceJoin.setOutputFormatClass(TextOutputFormat.class);

			FileOutputFormat.setOutputPath(jobReduceJoin, new Path(
					otherArgs[4]));

			System.exit(jobReduceJoin.waitForCompletion(true) ? 0
					: 2);

		}

	}
}