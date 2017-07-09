package com.sachin.election.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.sachin.election.constants.DataFileConstant;

public class BottomNConstituencyByStateYearPartyMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] tokens = value.toString().split(DataFileConstant.FILE_SEP);
		String generatedKey = tokens[DataFileConstant.STATE_NAME] + DataFileConstant.KEY_SEP
				+ tokens[DataFileConstant.YEAR] + DataFileConstant.KEY_SEP + tokens[DataFileConstant.PARTY_ABBREVATION];
		context.write(new Text(generatedKey), value);
	}// end of map
}