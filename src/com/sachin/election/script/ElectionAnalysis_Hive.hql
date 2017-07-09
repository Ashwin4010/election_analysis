use ElectionAnalysis;
Drop table AC_MIN_MAX_VOTES_T;
Drop table AC_VOTES_T;
Drop table BOTTOM_N_AC_BY_PARTY_T;
Drop table TOP_N_AC_BY_PARTY_T;
Drop table AC_VOTE_PERCENT_T;
create table AC_MIN_MAX_VOTES_T (STATE_NAME string, YEAR int, AC_NO int, AC_NAME string,AC_TYPE string, CAND_NAME string, CAND_SEX string, PARTY_NAME string, PARTY_ABBR string, TOT_VOTES_POLLED BIGINT, NO_OF_ELECTORS BIGINT, HIGH_LOW string) row format delimited fields terminated by "~" stored as textfile;
load data inpath '/output/election_analysis/pig/AC_MIN_MAX_VOTES_T/part-r-00000' into table AC_MIN_MAX_VOTES_T;
create table AC_VOTES_T (STATE_NAME string, YEAR int, PARTY_ABBR string,  PARTY_NAME string, TOT_VOTES_POLLED BIGINT, NO_OF_ELECTORS BIGINT) row format delimited fields terminated by "~" stored as textfile;
load data inpath '/output/election_analysis/pig/AC_VOTES_T/part-r-00000' into table AC_VOTES_T;
create table BOTTOM_N_AC_BY_PARTY_T (STATE_NAME string, YEAR int, PARTY_ABBR string,  PARTY_NAME string, AC_NO int, AC_NAME string,  VOTE_PERCENT double, order_by int) row format delimited fields terminated by "~" stored as textfile;
load data inpath '/output/election_analysis/pig/BOTTOM_N_AC_BY_PARTY_T/part-r-00000' into table BOTTOM_N_AC_BY_PARTY_T;
create table TOP_N_AC_BY_PARTY_T (STATE_NAME string, YEAR int, PARTY_ABBR string,  PARTY_NAME string, AC_NO int, AC_NAME string, VOTE_PERCENT double, order_by int) row format delimited fields terminated by "~" stored as textfile;
load data inpath '/output/election_analysis/pig/TOP_N_AC_BY_PARTY_T/part-r-00000' into table TOP_N_AC_BY_PARTY_T;
create table AC_VOTE_PERCENT_T (STATE_NAME string, YEAR int, PARTY_ABBR string,  PARTY_NAME string, TOT_VOTES_POLLED BIGINT, NO_OF_ELECTORS BIGINT, VOTE_PERCENT double) row format delimited fields terminated by "~" stored as textfile;
load data inpath '/output/election_analysis/pig/AC_VOTE_PERCENT_T/part-r-00000' into table AC_VOTE_PERCENT_T;

