ratings = LOAD 'u.data' USING PigStorage('\t') AS (user_id:int, movie_id:int, rating:float, timestamp:long);

movies = LOAD 'u.item' USING PigStorage('\t') AS (movie_id:int, title:chararray, release_date:chararray, video_date:chararray, imdb_url:chararray, unknown:int, action:int, adventure:int, animation:int, childrens:int, comedy:int, crime:int, documentary:int, drama:int, fantasy:int, film_noir:int, horror:int, musical:int, mystery:int, romance:int, scifi:int, thriller:int, war:int, western:int);

bajos = FILTER ratings BY rating < 2.0;

agrupado = GROUP bajos BY movie_id;
conteo = FOREACH agrupado GENERATE group AS movie_id, COUNT(bajos) AS total_bajos;

join1 = JOIN conteo BY movie_id, movies BY movie_id;
resultado = FOREACH join1 GENERATE movies::title AS titulo, conteo::total_bajos AS total_bajos;

con_rank = RANK resultado BY total_bajos DESC DENSE;

STORE con_rank INTO 'salida/peliculas_rating_bajo' USING PigStorage('\t');
