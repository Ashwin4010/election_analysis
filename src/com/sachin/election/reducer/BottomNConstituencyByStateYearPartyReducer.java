package com.sachin.election.reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.sachin.election.constants.DataFileConstant;

public class BottomNConstituencyByStateYearPartyReducer extends Reducer<Text, Text, Text, Text> {
	
	DecimalFormat df2 = new DecimalFormat( "#,###,###,##0.00" );
	int noOfRecord=5;
	@Override 
	 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
		int cnt=0;
		long votesPolled=0;
		long totalElectors=0;
		TreeMap<Double,Map<String,String>> inputMap= new TreeMap<>();
		
		Configuration conf = context.getConfiguration();
		String param = conf.get("noOfRecords");
		if(param!=null && !param.trim().equals("")){
			try{
				noOfRecord = Integer.parseInt(param);
			}catch(Exception e){
				noOfRecord =5;
			}
		}
		
		for(Text val: values){ 
		   String record= val.toString();
		   String [] valTokens = record.split(DataFileConstant.FILE_SEP); 
		   String voteStr = valTokens[DataFileConstant.TOTAL_VOTE_POLLED];
		   String noOfElectors = valTokens[DataFileConstant.NO_OF_ELECTORS];
		   if(StringUtils.isNotBlank(voteStr) && !voteStr.trim().equals("")){
			   try{
				   votesPolled= Long.parseLong(voteStr.trim()); 
			   }catch(Exception e){
				   votesPolled=0;
			   }
		   }
		   
		   if(StringUtils.isNotBlank(noOfElectors) && !noOfElectors.trim().equals("")){
			   try{
				   totalElectors= Long.parseLong(noOfElectors.trim()); 
			   }catch(Exception e){
				   totalElectors=0;
			   }
		   }
		   
		   Map<String, String> inputMap1=null;
		  // Double percentageOfVotes=0.00;
		   
		   
		   double percentageOfVotes = 0.00;
		  // double dd2dec = new Double(df2.format(dd)).doubleValue();

		   
		   
		   if(totalElectors>0){
			   double dd2dec = (votesPolled*100.00)/totalElectors;
			   percentageOfVotes = new Double(df2.format(dd2dec)).doubleValue();
		   }
		  
			   if(inputMap.containsKey(percentageOfVotes)){
				   inputMap1 =inputMap.get(percentageOfVotes);
			   }else{
				   inputMap1= new HashMap<>();
			   }
		 
		 if(percentageOfVotes>0.00){
		   String v3 = 
			   		valTokens[DataFileConstant.PARTY_NAME]
			   		+DataFileConstant.KEY_SEP
			   		+valTokens[DataFileConstant.ASSEMBLY_CONSTITUENCY_NO]
			   		+DataFileConstant.KEY_SEP
			   		+valTokens[DataFileConstant.ASSEMBLY_CONSTITUENCY_NAME]
					+DataFileConstant.KEY_SEP
			   		+percentageOfVotes;
		   
		   inputMap1.put(valTokens[DataFileConstant.ASSEMBLY_CONSTITUENCY_NO], v3);
		   
		  // System.out.println("percentageOfVotes="+v3+" "+percentageOfVotes);
		   inputMap.put(new Double(percentageOfVotes),inputMap1);
		 } 
		} // end of for 
		  
		Iterator<Map.Entry<Double, Map<String, String>>> iter = inputMap.entrySet().iterator();
		int count =0;
		while(iter.hasNext()){
			  count=count+1;
			  if(count<=5){
				  Map.Entry<Double, Map<String,String>> entry = iter.next();
				  Map<String, String> inputMapV3 = entry.getValue();
				  if(inputMapV3!=null){
					  Iterator<Map.Entry<String, String>> iter1 =inputMapV3.entrySet().iterator();
					  while(iter1.hasNext()){
						  Map.Entry<String,String> entry1 = iter1.next();
						  String newVal = entry1.getValue()+DataFileConstant.KEY_SEP+count;
						  context.write(key,new Text(newVal)); 
					  }
				  }
			  }else{
				  break;
			  }
		}
	 }// end of Reduce 

}
