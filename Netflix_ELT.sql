
------------------------------------------ DATA CLEANING ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS Netflix_RawData 
(	show_id		varchar(10) primary key,
	"type"		text,
	title		varchar(150),
	director	text, 
	"cast"		text,
	country		text,
	date_added	date,
	release_year int, 
	rating		varchar(15),
	duration	varchar(20),
	listed_in	text,
	description	text
)



select * from Netflix_RawData;
select * from Netflix_RawData order by title

-- checking if other non-english characters are showing up or not
select * from Netflix_RawData
where show_id = 's5023'  -- Amazing the korean characters are showing! 

-- checking for duplicates : >1
select show_id, COUNT(*)
from Netflix_RawData
group by show_id
having COUNT(*)>1 --  Show's no duplicates 

-- Just checking : Removing the duplicates using ctid to uniquely identify each row 
Delete from Netflix_RawData
where ctid not in (select min (ctid)
from Netflix_RawData
group by show_id
) -- show's 0 deleted which means no duplicates and I can safely make my show_id char

-- dropped the table and then created again to add the primary key 

-- checking for duplicates in the title
select * from Netflix_RawData
where title in (
select title
from Netflix_RawData
group by title
having COUNT(*)>1 
) -- shows no duplicates 

-- Since we have many values for single column it hinders our data analyzing abilities such as in listed-in we
-- we have international TV Show, TV dramas, TV Thrillers and this is the case for many columns we can split this

select show_id, trim(director_part) as director
from Netflix_RawData,
lateral unnest(string_to_array(director, ',')) AS director_part; -- splitting on the comma

-- inserting the new director table in Netflix_RawData
select show_id, trim (director_part) as director
into netflix_directors -- creating a new table named netflix_directors and inserting data splitted from directors there 
from  Netflix_RawData,
lateral unnest(string_to_array(director, ',')) AS director_part; 

select * from netflix_directors -- proper table created 

-- lets basically seperate cast, listed_in and country as well and create new table and directly insert 
select show_id,trim (cast_part) as single_cast_member
intp netflix_cast
from Netflix_RawData,
lateral unnest(string_to_array("cast", ',')) AS cast_part;

select * from netflix_cast


select show_id,Trim(listed_in_part) as genre
into  netflix_genre
from Netflix_RawData,
lateral unnest(string_to_array(listed_in, ',')) as listed_in_part

select * from netflix_genre

Select show_id,Trim(country_part) as  single_country
Into netflix_country
from Netflix_RawData,
lateral unnest(string_to_array(country, ',')) as country_part

select * from netflix_country

--- After running the isna() method in Jupyter we have lots of null values in director cast and country
-- For us populating country is very important 

select show_id, country
from Netflix_RawData
where country is null -- 831 rows are null 

select show_id, director
from Netflix_RawData 
where director is null -- 2634 rows are null 

select * from netflix_country where show_id = 's1001' -- if its null, its basically not showing

-- We need to somehow populate these null values : lets check for a specific director 

select * from netflix_RawData where director = 'Ahishor Solomon'  --- See one country is Null and other India
-- So most probably based on probablity it might be India as he is working with Indian directors 

-- lets get a combination of director, country to see how many other countries has  each director directed  

select director, single_country	
from netflix_country nc 
inner join netflix_directors nd on nc.show_id = nd.show_id -- telling sql to look for rows which have same id in both nc and nd 
group by director, single_country
-- shows how one director can direct in different countries as well example Aaron Sorkin, Aaron, woodley etc
-- and we will basically assumme that for this dierctor if there's any country that's null that probably one
-- of the country he has already directed in can be give, based on probality 

--- Pouplating the null values of country based on these probablities and map to director in our raw data
insert into netflix_country -- inserting these values in netflix_country table 
select show_id, m.single_country
from Netflix_RawData nr
inner join (select director, single_country	
from netflix_country nc 
inner join netflix_directors nd on nc.show_id = nd.show_id  
group by director, single_country
) m on nr.director = m.director
where nr.country is null

select * from netflix_country -- awesome populated null values 

--------------- Populating duration null values--------------

select * from Netflix_Rawdata 
where duration is null  -- see some glitch with data where duration is null and its values are in rating, so lets populate duration with rating wherever null


select *,
case 
when duration is null then rating else  duration  -- if else of sql started with the case 
end as  duration_or_rating
from Netflix_RawData; -- populated 

select show_id, duration
from Netflix_Rawdata -- confirmed population replacing null values 

--- As per our Pandas EDA, date added was also null so lets see what we can do about it
select show_id, date_added
from Netflix_RawData
where date_added is null -- only around 10 rows are null, so we can leave it instead of basically adding default values


-- Creating our Final clean table --
with cte as (
select *,
row_number() over (partition by title, type order by show_id) as rn
from Netflix_RawData
)
select show_id,"type", title, cast (date_added as date) as date_added, release_year, rating, duration,
description 
into Netflix_cleaned
from cte

select * from Netflix_cleaned --- whernever we need other columns such as country, cast, director we can basically join our table



------------------------------------------ DATA Analysis ----------------------------------------------------------

-- Answeing important analysis question and solving it with SQL query :
-- For each directors how many movies and shows they created in seperate columns 

select nd.director,
count (distinct case when n.type = 'Movie' then n.show_id end) as no_of_movies,
count (distinct case when n.type = 'TV Show' then n.show_id end) as no_of_tvshow
from Netflix_cleaned n
inner join netflix_directors nd on n.show_id = nd.show_id
group by nd.director
having count ( distinct n.type)>1 

-- done got a table with director names and no.of movies and tv shows they have directed seperately

-- Which country has produced highest number of comedy movies ?
select nc.single_country, count(distinct ng.show_id) as no_of_movies
from netflix_genre ng 
inner join netflix_country nc on ng.show_id = nc.show_id 
inner join netflix_cleaned n on ng.show_id = n.show_id
where ng.genre = 'Comedies' and n.type = 'Movie'
group by nc.single_country
order by no_of_movies desc-- Got US as having the highest no.of comedy movies on top of the table
limit 1 -- postgre_sql syntax for getting the top 1

-- For each year, which director has maximum number of movies released 
with cte as (
select nd.director, extract(year from n.date_added) as date_year, count( n.show_id) as no_of_movies -- postgre sql syntax extract
from Netflix_Cleaned n 
inner join netflix_directors nd on n.show_id = nd.show_id
where type = 'Movie'
group by nd.director, extract(year from n.date_added) 
), 
cte2 as (
select *, 
row_number () over (partition by date_year order by no_of_movies desc, director ) as rn 
from cte 
)

select * from cte2 where rn=1 -- we got it from 2008 to 2021 

-- What's the avg duration of movies for each genre 
select ng.genre, floor(avg(cast(replace(n.duration, ' min', '') as int))) as avg_duration
from netflix_cleaned n
inner join netflix_genre ng on n.show_id = ng.show_id
where n.type = 'Movie' 
and n.duration ~ '^[0-9]+ min$'
group by ng.genre; -- yes we got it 


-- Find the list of directors who have created horror and comedy movies both and display director names along with no. of horror and comedy movies 
select nd.director, 
count(distinct case when ng.genre = 'Comedies' then n.show_id end) as no_of_comedy,
count(distinct case when ng.genre = 'Horror Movies' then n.show_id end) as no_of_horror
from netflix_cleaned n
inner join netflix_genre ng on n.show_id = ng.show_id
inner join netflix_directors nd on n.show_id = nd.show_id
where n.type = 'Movie' and ng.genre in ('Comedies', 'Horror Movies')
group by nd.director
having count(distinct ng.genre) = 2; -- done 

