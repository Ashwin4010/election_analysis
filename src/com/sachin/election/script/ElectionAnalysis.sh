#############################  POC - State Election Data Analysis/Processing using Hadoop ecosystem   ##############################
#############################  By -  Sachin Sanke                                                      ##############################
#############################  Batch-Feb-2017                                                  	  ##############################
############################# (.txt file processing separated with tab)  #####################################
DATE=$(date +"%Y%m%d_%H%M%S")
####LOGFILE="/home/notroot/lab/logs/"$DATE".log"
####### Delete the output directories ##################################
hdfs dfs -rmr /output/election_analysis;
hdfs dfs -mkdir /output/election_analysis;
hdfs dfs -mkdir /output/election_analysis/pig;
##############Start MapReduce program to process  .txt file for State election  ##############################
##################################################################################
echo "********************************************Starting MapReduce program***************************************"
echo "********************************************Started MapReduce  - AssemblyYearMinMaxDriver ********************" 
hadoop jar /home/notroot/lab/programs/election/electionanalysis.jar com.sachin.election.driver.AssemblyYearMinMaxDriver /input/India_state_election_data.txt /output/election_analysis/AC_MIN_MAX_VOTES_T;
echo "********************************************Finished MapReduce processing - AssemblyYearMinMaxDriver *********" 
echo "********************************************Started MapReduce  - AssemblyStateYearPartyWiseVotesDriver *******" 
hadoop jar /home/notroot/lab/programs/election/electionanalysis.jar com.sachin.election.driver.AssemblyStateYearPartyWiseVotesDriver /input/India_state_election_data.txt /output/election_analysis/AC_VOTES_T;
echo "********************************************Finished MapReduce processing - AssemblyStateYearPartyWiseVotesDriver *******************************"
echo "********************************************Started MapReduce  - BottomNConstituencyByStateYearPartyDriver ****************************************"
hadoop jar /home/notroot/lab/programs/election/electionanalysis.jar com.sachin.election.driver.BottomNConstituencyByStateYearPartyDriver /input/India_state_election_data.txt /output/election_analysis/BOTTOM_N_AC_BY_PARTY_T 5;
echo "*********************************************Finished MapReduce processing - BottomNConstituencyByStateYearPartyDriver***************************" 
echo "*********************************************Started MapReduce  - TopNConstituencyByStateYearPartyDriver*****************************************"
hadoop jar /home/notroot/lab/programs/election/electionanalysis.jar com.sachin.election.driver.TopNConstituencyByStateYearPartyDriver /input/India_state_election_data.txt /output/election_analysis/TOP_N_AC_BY_PARTY_T 5;
echo "*********************************************Finished MapReduce processing - TopNConstituencyByStateYearPartyDriver *****************************"
echo "**********************************************Started MapReduce  - VotingPercentByStateYearPartyDriver********************************************"
hadoop jar /home/notroot/lab/programs/election/electionanalysis.jar com.sachin.election.driver.VotingPercentByStateYearPartyDriver /input/India_state_election_data.txt /output/election_analysis/AC_VOTE_PERCENT_T;
echo "**********************************************Finished MapReduce processing - VotingPercentByStateYearPartyDriver*********************************"
echo "************************************Finished MapReduce Processing ***********************************************************************************************"
echo "*********************************************************************************************************************************************************"
################################################################################
############# PROCESSING DATA USING PIG ##########################################
################################################################################
echo "****************************Started PIG Processing********************************************************************************************* "
pig -f /home/notroot/lab/script/ElectionAnalysis.pig;
echo "****************************Finished PIG processing successfully ******************************************************************************"
################################################################################
############# IMPORTING DATA in SQOOP ##########################################
################################################################################
echo "********************************Started Importing the data to MYSQL  using SQOOP ***************************************************************"
##### Creating the tables in MySql
sqoop eval --connect jdbc:mysql://localhost/ElectionAnalysis -username root -password admin --query "use ElectionAnalysis;";
sqoop eval --connect jdbc:mysql://localhost/ElectionAnalysis -username root -password admin --query "grant all privileges on ElectionAnalysis.* to '%'@'localhost'";
sqoop eval --connect jdbc:mysql://localhost/ElectionAnalysis -username root -password admin --query "grant all privileges on ElectionAnalysis.* to ''@'localhost'";
sqoop eval --connect jdbc:mysql://localhost/ElectionAnalysis -username root -password admin --query "drop table if exists AC_MIN_MAX_VOTES_T";
sqoop eval --connect jdbc:mysql://localhost/ElectionAnalysis -username root -password admin --query "drop table if exists AC_VOTES_T";
sqoop eval --connect jdbc:mysql://localhost/ElectionAnalysis -username root -password admin --query "drop table if exists BOTTOM_N_AC_BY_PARTY_T";
sqoop eval --connect jdbc:mysql://localhost/ElectionAnalysis -username root -password admin --query "drop table if exists TOP_N_AC_BY_PARTY_T";
sqoop eval --connect jdbc:mysql://localhost/ElectionAnalysis -username root -password admin --query "drop table if exists AC_VOTE_PERCENT_T";
echo "*************************************Started Creating Tables in MYSQL using Sqoop *****************************************************************"
sqoop eval --connect jdbc:mysql://localhost/ElectionAnalysis -username root -password admin --query "create table AC_MIN_MAX_VOTES_T (STATE_NAME varchar (100), YEAR int, AC_NO int, AC_NAME varchar(100),AC_TYPE varchar(10),CAND_NAME varchar(250),CAND_SEX varchar(10), PARTY_NAME varchar(100), PARTY_ABBR varchar(30),TOT_VOTES_POLLED long,NO_OF_ELECTORS long,HIGH_LOW varchar(2))";
sqoop eval --connect jdbc:mysql://localhost/ElectionAnalysis -username root -password admin --query "create table AC_VOTES_T (STATE_NAME varchar (100), YEAR int,  PARTY_ABBR varchar(30), PARTY_NAME varchar(100), TOT_VOTES_POLLED long,NO_OF_ELECTORS long)";
sqoop eval --connect jdbc:mysql://localhost/ElectionAnalysis -username root -password admin --query "create table BOTTOM_N_AC_BY_PARTY_T (STATE_NAME varchar (100), YEAR int,  PARTY_ABBR varchar(30), PARTY_NAME varchar(100), AC_NO int, AC_NAME varchar(100),VOTE_PERCENT double, order_by int)";
sqoop eval --connect jdbc:mysql://localhost/ElectionAnalysis -username root -password admin --query "create table TOP_N_AC_BY_PARTY_T (STATE_NAME varchar (100), YEAR int,  PARTY_ABBR varchar(30), PARTY_NAME varchar(100), AC_NO int, AC_NAME varchar(100),VOTE_PERCENT double, order_by int)";
sqoop eval --connect jdbc:mysql://localhost/ElectionAnalysis -username root -password admin --query "create table AC_VOTE_PERCENT_T (STATE_NAME varchar (100), YEAR int,  PARTY_ABBR varchar(30), PARTY_NAME varchar(100), TOT_VOTES_POLLED long, NO_OF_ELECTORS long,VOTE_PERCENT double)";
echo "*************************************Started data exporting to MySQL tables using sqoop *************************************************************"
#### exporting the data into MYSQL
sqoop export --connect jdbc:mysql://localhost/ElectionAnalysis -username root -password admin --table AC_MIN_MAX_VOTES_T --export-dir /output/election_analysis/pig/AC_MIN_MAX_VOTES_T/part-r-00000 --fields-terminated-by '~';
sqoop export --connect jdbc:mysql://localhost/ElectionAnalysis -username root -password admin --table AC_VOTES_T --export-dir /output/election_analysis/pig/AC_VOTES_T/part-r-00000 --fields-terminated-by '~';
sqoop export --connect jdbc:mysql://localhost/ElectionAnalysis -username root -password admin --table BOTTOM_N_AC_BY_PARTY_T --export-dir /output/election_analysis/pig/BOTTOM_N_AC_BY_PARTY_T/part-r-00000 --fields-terminated-by '~';
sqoop export --connect jdbc:mysql://localhost/ElectionAnalysis -username root -password admin --table TOP_N_AC_BY_PARTY_T --export-dir /output/election_analysis/pig/TOP_N_AC_BY_PARTY_T/part-r-00000 --fields-terminated-by '~';
sqoop export --connect jdbc:mysql://localhost/ElectionAnalysis -username root -password admin --table AC_VOTE_PERCENT_T --export-dir /output/election_analysis/pig/AC_VOTE_PERCENT_T/part-r-00000 --fields-terminated-by '~';
echo "*************************************Exporting of data to MYSQL is done ******************************************************************************"
################################################################################
############# IMPORTING DATA in HIVE ##########################################
################################################################################
echo "*************************************Started creating of hive tables *********************************************************************************"
hive -f /home/notroot/lab/script/ElectionAnalysis_Hive.hql;
echo "**************************************Hive process is done ********************************************************************************************"
exit;