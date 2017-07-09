package com.sachin.election.driver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sachin.election.mapper.AssemblyYearMinMaxMapper;
import com.sachin.election.reducer.AssemblyYearMinMaxReducer; 

public class AssemblyYearMinMaxDriver {
	 @SuppressWarnings("deprecation")
		public static void main(String[] args) throws Exception {          
			 Configuration conf = new Configuration();                 
			 Job job = new Job(conf, "Min/Max Vote Counter");  
			
			 //Ignore the warning as new techniques for getting handle to Job is available now.             
			 job.setJarByClass( AssemblyYearMinMaxDriver.class );       
			 job.setMapperClass( AssemblyYearMinMaxMapper.class );       
			 job.setReducerClass( AssemblyYearMinMaxReducer.class );             
			 job.setMapOutputKeyClass( Text.class );       
			 job.setMapOutputValueClass( Text.class );             
			 job.setOutputKeyClass( Text.class );            
			 job.setOutputValueClass( Text.class );              
			 FileInputFormat.addInputPath( job, new Path( args[0] ) );      
			 FileOutputFormat.setOutputPath( job, new Path( args[1] ) );              
			 System.exit( job.waitForCompletion( true ) ? 0 : 1 );    
		} 
}
