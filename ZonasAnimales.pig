animales = LOAD 'ANIMALES.data' USING PigStorage('\t') AS (id_animal:chararray, id_edificio:chararray, fecha:chararray, tipo:chararray);

edificios = LOAD 'EDIFICIOS.data' USING PigStorage('\t') AS (id_edificio:chararray, id_zona:chararray, capacidad:int);

join1 = JOIN animales BY id_edificio, edificios BY id_edificio;

Zonajoin = FOREACH join1 GENERATE edificios::id_zona AS id_zona;

agrupado = GROUP Zonajoin BY id_zona;

conteo = FOREACH agrupado GENERATE group AS id_zona, COUNT(Zonajoin) AS total;

conteo_rank = RANK conteo BY total DESC DENSE;

STORE conteo_rank INTO 'salida/zonas_animales' USING PigStorage('\t');
