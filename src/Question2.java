import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Question2 {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		Text ageGr = new Text();
		Text gender =new Text();
	
		@Override
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
			String line = value.toString();
		    String tokens[]=line.split("\\::");
		    
		    int age=Integer.parseInt(tokens[2].trim());
		    
		    if(age>0 && age<18){
		    	 ageGr.set(7+"");
		    }else if(age>=18 && age<=24){
		    	ageGr.set(24+"");
		    }else if(age>=25 && age<=34){
		    	ageGr.set(31+"");
		    }else if(age>=35 && age<=44){
		    	ageGr.set(41+"");
		    }else if(age>=45 && age<=55){
		    	ageGr.set(51+"");
		    }else if(age>=56 && age<=61){
		    	ageGr.set(56+"");
		    }else if(age>=62){
		    	ageGr.set(62+"");
		    }
		   
		    gender.set(tokens[1].trim());
		    
		    context.write(ageGr,gender);
		    
		}
	}

	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		
		Text gCount=new Text();
	
		@Override
	    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

			int mCount=0,fCount=0;
			Iterator<Text> i=values.iterator();
		    while(i.hasNext()){
				String gender=i.next().toString();
				if(gender.equals("M")){
					mCount++;
				}else if(gender.equals("F")){
					fCount++;
				}
			}

			gCount.set("M	"+mCount);
		    context.write(key,gCount);
		    
		    gCount.set("F	"+fCount);
		    context.write(key,gCount);
	    }
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "question2");
	    job.setJarByClass(Question2.class);
	    
	    job.setMapperClass(Map.class);
	    //job.setCombinerClass(Reduce.class);
	    job.setReducerClass(Reduce.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);	
   }

}
