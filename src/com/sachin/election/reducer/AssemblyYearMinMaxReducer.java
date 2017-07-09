package com.sachin.election.reducer;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.sachin.election.constants.DataFileConstant;

public class AssemblyYearMinMaxReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int maxVotes = 0;
		int minVotes = 0;
		int votes = 0;
		int cnt = 0;
		Text maxVoteRecord = null;
		Text minVoteRecord = null;
		String maxKey = "";
		String minKey = "";

		for (Text val : values) {
			String record = val.toString();
			String[] valTokens = val.toString().split(DataFileConstant.FILE_SEP);
			String voteStr = valTokens[DataFileConstant.TOTAL_VOTE_POLLED];
			if (StringUtils.isNotBlank(voteStr)) {
				votes = Integer.parseInt(valTokens[DataFileConstant.TOTAL_VOTE_POLLED]);
			} else {
				votes = 0;
			}

			if (votes > maxVotes) {
				maxVoteRecord = new Text(record);
				;
				maxVotes = votes;

			} // end of if

			if (cnt == 0) {
				minVotes = votes;
				minVoteRecord = new Text(record);
			} else {
				if (votes < minVotes) {
					minVotes = votes;
					minVoteRecord = new Text(record);
					;
				}
			}
			cnt = cnt + 1;
		} // end of for

		context.write(minVoteRecord, new Text("L"));
		context.write(maxVoteRecord, new Text("H"));
	}// end of Reduce

}
