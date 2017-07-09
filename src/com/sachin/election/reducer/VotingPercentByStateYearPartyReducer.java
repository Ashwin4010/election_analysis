package com.sachin.election.reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.sachin.election.constants.DataFileConstant;

public class VotingPercentByStateYearPartyReducer  extends Reducer<Text, Text, Text, Text> {
	DecimalFormat df2 = new DecimalFormat( "#,###,###,##0.00" );
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
		   partyName = valTokens[DataFileConstant.PARTY_NAME];
		   
		   String key1 = valTokens[DataFileConstant.STATE_NAME]
				   		+DataFileConstant.KEY_SEP
				   		+valTokens[DataFileConstant.YEAR]
				   		+DataFileConstant.KEY_SEP
				   		+valTokens[DataFileConstant.ASSEMBLY_CONSTITUENCY_NO]
				   		+DataFileConstant.KEY_SEP
				   		+valTokens[DataFileConstant.PARTY_ABBREVATION];
		   if(StringUtils.isNotBlank(voteStr) && !voteStr.trim().equals("")){
			   votesPolled = votesPolled+ Long.parseLong(voteStr.trim()); 
		   }else{
			   votesPolled=votesPolled+0;
		   }
		   
		   if(StringUtils.isNotBlank(noOfElectors) && !noOfElectors.trim().equals("")){
			   if(!inputMap.containsKey(key1)){
				   totalElectors = totalElectors+ Long.parseLong(noOfElectors.trim());
			   }
		   }else{
			   totalElectors=totalElectors+0;
		   }
		    
		  } // end of for 
		  
		  double dd2dec = (votesPolled*100.00)/totalElectors;
		 Double percentage = new Double(df2.format(dd2dec)).doubleValue();
		  
		 // double percentage= (votesPolled*100)/totalElectors;
		  context.write(key,new Text(partyName+DataFileConstant.KEY_SEP+votesPolled+DataFileConstant.KEY_SEP
				  +totalElectors+DataFileConstant.KEY_SEP+percentage)); 
	 
	 }// end of Reduce 
}
