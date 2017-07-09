A = LOAD '/output/election_analysis/AC_MIN_MAX_VOTES_T/part-r-00000' using PigStorage ('\t') as (STATE_NAME:chararray, YEAR:int, AC_NO:int, AC_NAME:chararray ,AC_TYPE:chararray, CAND_NAME:chararray,CAND_SEX:chararray, PARTY_NAME:chararray, PARTY_ABBR:chararray,TOT_VOTES_POLLED:long, NO_OF_ELECTORS:long,HIGH_LOW:chararray);
orSYAN = ORDER A by STATE_NAME, YEAR, AC_NO;
STORE orSYAN into '/output/election_analysis/pig/AC_MIN_MAX_VOTES_T' using PigStorage('~');

A = LOAD '/output/election_analysis/AC_VOTES_T/part-r-00000' using PigStorage ('\t') as (STATE_NAME:chararray, YEAR:int,  PARTY_ABBR:chararray,PARTY_NAME:chararray, TOT_VOTES_POLLED:long,NO_OF_ELECTORS:long);
orSYPA = ORDER A by STATE_NAME, YEAR, PARTY_ABBR;
STORE orSYPA into '/output/election_analysis/pig/AC_VOTES_T' using PigStorage('~');

A = LOAD '/output/election_analysis/BOTTOM_N_AC_BY_PARTY_T/part-r-00000' using PigStorage ('\t') as (STATE_NAME:chararray, YEAR:int,PARTY_ABBR:chararray,PARTY_NAME:chararray,AC_NO:int,AC_NAME:chararray,VOTE_PERCENT:double,ORDER:int);
orSYAN = ORDER A by STATE_NAME,YEAR, AC_NO;
STORE orSYAN into '/output/election_analysis/pig/BOTTOM_N_AC_BY_PARTY_T ' using PigStorage('~');

A = LOAD '/output/election_analysis/TOP_N_AC_BY_PARTY_T/part-r-00000' using PigStorage ('\t') as (STATE_NAME:chararray, YEAR:int,PARTY_ABBR:chararray,PARTY_NAME:chararray,AC_NO:int,AC_NAME:chararray,VOTE_PERCENT:double,ORDER:int);
orSYAN = ORDER A by STATE_NAME,YEAR, AC_NO;
STORE orSYAN into '/output/election_analysis/pig/TOP_N_AC_BY_PARTY_T' using PigStorage('~');

A = LOAD '/output/election_analysis/AC_VOTE_PERCENT_T/part-r-00000' using PigStorage ('\t') as (STATE_NAME:chararray, YEAR:int,PARTY_ABBR:chararray,PARTY_NAME:chararray,TOT_VOTES_POLLED:long,NO_OF_ELECTORS:long,VOTE_PERCENT:double);
orSYPA = ORDER A by STATE_NAME,YEAR, PARTY_ABBR;
STORE orSYPA into '/output/election_analysis/pig/AC_VOTE_PERCENT_T' using PigStorage('~');