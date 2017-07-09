package com.sachin.election.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sachin.election.mapper.TopNConstituencyByStateYearPartyMapper;
import com.sachin.election.reducer.TopNConstituencyByStateYearPartyReducer;

public class TopNConstituencyByStateYearPartyDriver {
	public static int noOfRecords =5;
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {          
		 Configuration conf = new Configuration();   
		 if(args!=null && args.length>2){
			 if(args[2]!=null && !args[2].trim().equals("")){
				 try{
					  noOfRecords=Integer.parseInt(args[2].trim());
					 
				 }catch(Exception e){
					 noOfRecords =5;
				 }
			 }
		 }
		 conf.set("noOfRecords", noOfRecords+"");
		 Job job = new Job(conf, "TopNConstituencyByStateYearParty");  
		 job.setJarByClass( TopNConstituencyByStateYearPartyDriver.class);       
		 job.setMapperClass(TopNConstituencyByStateYearPartyMapper.class);       
		 job.setReducerClass( TopNConstituencyByStateYearPartyReducer.class);             
		 job.setMapOutputKeyClass( Text.class );       
		 job.setMapOutputValueClass( Text.class );             
		 job.setOutputKeyClass( Text.class );            
		 job.setOutputValueClass( Text.class );     
		 
		 FileInputFormat.addInputPath( job, new Path( args[0] ) );      
		 FileOutputFormat.setOutputPath( job, new Path( args[1] ) );              
		 System.exit( job.waitForCompletion( true ) ? 0 : 1 );    
	} 
}
