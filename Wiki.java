package id2221.wiki;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.ArrayList;

import java.util.Scanner;
import java.io.Writer;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.File;
import java.io.FileInputStream;
import java.lang.StringBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Wiki {

		public static class WikiMapper extends Mapper<Object, Text, Text, Text> {

			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				/*
				Each record will be a single article in a single line. The title looks the same but not 
				the isbn references unfortunately.
				Here are some example parts of these lines:
				...<title>Josiah Dubois</title>... 	(Exists in all pages)
				...isbn = 0-946098-36-0...
				...isbn = 0-946098-36-X...
				...isbn=0-946098-36-0...
				...isbn= 0-946098-36-0...
				...isbn= }... 				(Missing isbn)
				...ISBN|0-946098-36-0...		(Capital ISBN)
				...ISBN=0946 09836 0...
				...ISBN=978-946098-36-0...
				...no ISBN number...

				The weird part is that isbn numbers are different length and structure. I think the best practice 
				would be to concatinate all the numbers so that 0-946098-36-0 -> 0946098360. By doing so, we can be 
				sure to accurately match other articles with the same isbn but different structure.
				*/
				String val = value.toString(); //String has more useful methods than Text from hadoop

				Text articleName; //here we save the article name
				int index = val.indexOf("<title>"); //get index of that tag
				if(index == -1) {
					return; //Could not find <title> in wikipedia page, is even a page?	
				}
				articleName = new Text(val.substring(index+7,val.indexOf("</title>",index))); //<title>Alan Turing</title>
				//System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"+ articleName.toString() + "!!!!!!!!!!!!!!!!!!!!");
				//now let us find the isbn references, this is the tricky part
				for(; index >= 0 ; index = Math.min(val.indexOf("isbn",index+1),val.indexOf("ISBN",index+1))){
					int stopCount = 0; //If two non-allowed chars appear in sequence, then stop!
					index = index+4; //at the end of "isbn" or "ISBN"
					while(val.charAt(index) == ' '){index++;} //remove pre-spaces
					if(val.charAt(index) != '|' && val.charAt(index) != '='){continue;}
					index++; //continue past the = or |
					while(val.charAt(index) == ' '){index++;} //remove post-spaces
					
					StringBuilder isbn = new StringBuilder(""); //we will store the isbn here
					char c = val.charAt(index);
					while(c == '-' || Character.isLetterOrDigit(c)){
						if(c != '-'){
							isbn.append(c);
						}
						index++;
						c = val.charAt(index);					
					}
					if(isbn.length()!=10 && isbn.length()!=13){continue;} //something went wrong, isbn is either 10 or 13
					//System.out.println("###################"+isbn.toString()+"########################");
					context.write(new Text(isbn.toString()),articleName); //write!
				}
			}

		}

		public static class WikiReducer extends Reducer<Text, Text, Text, Text> {

			TreeMap<Text, ArrayList<Text>> ISBN_Articles_Map  = new TreeMap<Text, ArrayList<Text>>();
			

			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				//The key here is the ISBN and the Value are the article names
				//So we are expecting (isbn, [articlename1,articlename2,articlename3...])
				/*
				System.out.print("Isbn: " + key.toString() + " found in:");
				for(Text v : values){System.out.print(" - " + v.toString());}
				System.out.println("");*/

				Text ISBN = new Text(key.toString()); //this is our isbn val
				if(!ISBN_Articles_Map.containsKey(ISBN)){
						ISBN_Articles_Map.put(ISBN, new ArrayList<Text>()); // init the array list	
				}
			
				for(Text article : values){
					if(!ISBN_Articles_Map.get(ISBN).contains(article)){
						ISBN_Articles_Map.get(ISBN).add(new Text(article.toString())); //collect the articles for the isbn
					}
				}
			}
			
			protected void cleanup(Context context) throws IOException, InterruptedException {
		    		//Here we have stored all the ISBN_to_ArticlesMap
				//We 
				
				for(Text ISBN : ISBN_Articles_Map.keySet()){
					if(ISBN_Articles_Map.get(ISBN).size() < 3){
						continue; //Skip the ISBN that have less than three articles mentioning them
					}
					System.out.print("Isbn: " + ISBN.toString() + " found in:");
					for(Text article : ISBN_Articles_Map.get(ISBN)){
						System.out.print(" - " + article.toString());
						//TODO: Write to the output by doing context.write(ISBN,new Text());
						context.write(ISBN,article);
					}
					System.out.println("");
				}
			}
		}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		//Scan scan = new Scan(); 
		Job job = Job.getInstance(conf,"wiki");
		//job.setNumReduceTasks(1);
		job.setJarByClass(Wiki.class);

		job.setMapperClass(WikiMapper.class);
		job.setCombinerClass(WikiReducer.class);
		job.setReducerClass(WikiReducer.class);
	
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
    }
	
	
}
