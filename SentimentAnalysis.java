import java.io.IOException;
import java.util.StringTokenizer;
import java.io.File;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SentimentAnalysis {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		private static IntWritable Length = new IntWritable(); 
		private Text word = new Text(); 
		private Text final_sen = new Text(); 
		int counter = 0;
		
		String st = "Text="; 
		String en = "CreationDate="; 

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			String new_comment = ""; 
			int isFound; 
			int endFound; 
			boolean found = true;
			String only_comment = "";
			String word_com = "";
			
			while (itr.hasMoreTokens()) { 
				word.set(itr.nextToken()); 
				isFound = (word.find(st)); 
				while (isFound != -1) { 
					word_com = word.toString(); 
					word_com += " "; 
					only_comment = only_comment + word_com; 
					word.set(itr.nextToken()); 
					endFound = (word.find(en)); 
					if (endFound != -1) { 
						isFound = -1; 
						found = false; 
					}
				}
				if (found == false) 
					break;
			}
			for (int i = 6; i < only_comment.length(); i++) { 
				char c = only_comment.charAt(i);
				new_comment += c;
			}
			counter += 1; 
			final_sen = new Text(new_comment); 
			Length = new IntWritable(counter); 
			context.write(final_sen, Length); 

		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		int overall_pos = 0; 
		int overall_neg = 0; 
		String key_text;
		String key_word = ""; 
		int total_count = 0; 
		private IntWritable result = new IntWritable();
		private Text sentiment = new Text();
		private Text overall_status = new Text();
		private Text token = new Text();

		
		public String[] make_token(String str, int size) {

			String[] arrOfStr = str.split("~", size);
			return arrOfStr;
		}

		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			total_count += 1; 
			String positive_arr = ""; 
			String negative_arr = ""; 
			int pos_count = 0; 
			int neg_count = 0; 
			String path1 = "/home/lakshminath/positive.txt";
			String path2 = "/home/lakshminath/negative.txt";
			
			String a;
			File kw = new File(path1);
			Scanner myReader = new Scanner(kw);
			while (myReader.hasNextLine()) {
				a = myReader.nextLine();
				positive_arr += a;
				positive_arr += "~";
			}
			myReader.close();
			
			String b;
			File kw1 = new File(path2);
			Scanner myReader1 = new Scanner(kw1);
			while (myReader1.hasNextLine()) {
				b = myReader1.nextLine();
				negative_arr += b;
				negative_arr += "~";
			}
			myReader1.close();
			
			String[] pos_file = make_token(positive_arr, 2007);
			String[] neg_file = make_token(negative_arr, 4783);

			
			key_text = key.toString();

			StringTokenizer tokens = new StringTokenizer(key.toString());
			if (key_text.contains(key_word)) { 
				while (tokens.hasMoreTokens()) { 
					token.set(tokens.nextToken());
					String check = token.toString(); 
					check = check.replaceAll("[^a-zA-Z0-9]", "");
					check = check.toLowerCase();

					for (int i = 0; i < pos_file.length; i++) {
						if (pos_file[i].equals(check)) { 
							pos_count += 1; 
						}
					}
					for (int i = 0; i < neg_file.length; i++) {
						if (neg_file[i].equals(check)) 
							neg_count += 1; 
					}
				}
				
				if (pos_count > neg_count) {
					overall_pos += 1; 
					sentiment = new Text("Positive Comment");
					overall_status = new Text("Overall the key word is positive");
				}
				
				else if (pos_count < neg_count) {
					overall_neg += 1; 
					sentiment = new Text("Negative Comment");
					overall_status = new Text("Overall the key word is negative");
				}

				context.write(key, null);
				context.write(sentiment, null); 
			}
			if (total_count == 10) { 
				if (overall_neg > overall_pos) { 
					overall_status = new Text("Overall the key word is negative");
				}
				if (overall_neg < overall_pos) { 
					overall_status = new Text("Overall the key word is positive");
				}
				context.write(overall_status, result); 
				context.write(new Text("Sentiment Score = "+((double)(overall_pos-overall_neg)/(overall_pos+overall_neg))+""), null);
				context.write(new Text("Positivity Score = "+((double)(overall_pos)/(overall_pos+overall_neg))+""), null);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Task Sentiments");
		job.setJarByClass(SentimentAnalysis.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
