SELECT * FROM big_data_analysis.data_wordcount;

create table big_data_analysis.ai as 
(select time, count as ai from big_data_analysis.data_wordcount where word = 'ai');

create table big_data_analysis.data as 
(select time, count as data from big_data_analysis.data_wordcount where word = 'data');

create table big_data_analysis.good as 
(select time, count as good from big_data_analysis.data_wordcount where word = 'good');

create table big_data_analysis.movie as 
(select time, count as movie from big_data_analysis.data_wordcount where word = 'movie');

create table big_data_analysis.spark as
(select time, count as spark from big_data_analysis.data_wordcount where word = 'spark');

create table word_count_res as (select 
	ai.time, 
    ai, 
    ifnull(data, 0) as data, 
    ifnull(good,0) as good, 
    ifnull(movie, 0) as movie,
    ifnull(spark, 0) as spark
from 
((((ai left join data on ai.time = data.time) 
left join good on ai.time = good.time)
left join movie on ai.time = movie.time)
left join spark on ai.time = spark.time));