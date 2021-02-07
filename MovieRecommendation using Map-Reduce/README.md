/*
Map-reduce in Java

Movie recommendation using reviews given by users.
Correlation between two movies is used to recommend a movie.
A data set contains user_id, movie_id, rating, timestamp.
One user can review many movies.
Find the correlation between pairs of movies reviewed by same users.
eg. 	usr1 reviewed mv1 and mv2.
	usr2 reviewed mv1 and mv2.
	usr3 reviewed mv1 and mv2.
	-------------------------
find the correlation between mv1 and mv2 

Data is stored in hadoop.
Map-reduce job is performed in Hadoop
Chained map-reduce job is performed where output of first reducer is given as input to second mapper.
Custom writables are used.

*/
