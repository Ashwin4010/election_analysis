package com.sachin.election.reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.sachin.election.constants.DataFileConstant;

public class AssemblyStateYearPartyWiseVotesReducer extends Reducer<Text, Text, Text, Text> {
	@Override 
	 public void reduce(Text key, Iterable<Text> values, Context context) 
	   throws IOException, InterruptedException { 
		int cnt=0;
		long votesPolled=0;
		long totalElectors=0;
		String partyName="";
		Map<String,Integer> inputMap= new HashMap<>();
		  for(Text val: values){ 
		   String record= val.toString();
		   String [] valTokens = val.toString().split(DataFileConstant.FILE_SEP); 
		   String voteStr = valTokens[DataFileConstant.TOTAL_VOTE_POLLED];
		   String noOfElectors = valTokens[DataFileConstant.NO_OF_ELECTORS];
		   
		   partyName=valTokens[DataFileConstant.PARTY_NAME];
		   String key1 = valTokens[DataFileConstant.STATE_NAME] +DataFileConstant.KEY_SEP
				   		+valTokens[DataFileConstant.YEAR] +DataFileConstant.KEY_SEP
				   		+valTokens[DataFileConstant.ASSEMBLY_CONSTITUENCY_NO]
				   		+DataFileConstant.KEY_SEP
				   		+valTokens[DataFileConstant.PARTY_ABBREVATION];
		   if(StringUtils.isNotBlank(voteStr)){
			   votesPolled = votesPolled+ Long.parseLong(valTokens[DataFileConstant.TOTAL_VOTE_POLLED]); 
		   }else{
			   votesPolled=votesPolled+0;
		   }
		   
		   if(StringUtils.isNotBlank(noOfElectors)){
			   if(!inputMap.containsKey(key1)){
				   totalElectors = totalElectors+ Long.parseLong(valTokens[DataFileConstant.NO_OF_ELECTORS]);
			   }
		   }else{
			   totalElectors=totalElectors+0;
		   }
		    
		  } // end of for 
	 
	  context.write(key,new Text(partyName+DataFileConstant.KEY_SEP+votesPolled+DataFileConstant.KEY_SEP+totalElectors)); 
	 
	 }// end of Reduce 
}
