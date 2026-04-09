tareas = LOAD 'TAREA_GANADERIA.data' USING PigStorage('\t') AS (id_tarea:chararray, id_dron:chararray, id_animal:chararray, id_suministro:chararray, fecha:chararray, accion:chararray);

animales = LOAD 'ANIMALES.data' USING PigStorage('\t') AS (id_animal:chararray, id_edificio:chararray, nacimiento:chararray, especie:chararray);

suministros = LOAD 'SUMINISTRO.data' USING PigStorage('\t') AS (id_suministro:chararray, id_tipo:chararray, vencimiento:chararray, fabricado:chararray);

tiposSum = LOAD 'TIPO_SUMINISTRO.data' USING PigStorage('\t') AS (id_tipo:chararray, nombre:chararray, categoria:chararray, precio:float, cantidad:int);

join1 = JOIN tareas BY id_animal, animales BY id_animal;
tarea_especie = FOREACH join1 GENERATE tareas::id_suministro AS id_suministro, animales::especie AS especie;

join2 = JOIN tarea_especie BY id_suministro, suministros BY id_suministro;
tar_esp_tipo = FOREACH join2 GENERATE suministros::id_tipo AS id_tipo, tarea_especie::especie AS especie;

join3 = JOIN tar_esp_tipo BY id_tipo, tiposSum BY id_tipo;
con_precio = FOREACH join3 GENERATE tar_esp_tipo::especie AS especie, tiposSum::precio AS precio;

agrupado = GROUP con_precio BY especie;
totales = FOREACH agrupado GENERATE group AS especie, SUM(con_precio.precio) AS total_precio;

con_rank = RANK totales BY total_precio DESC DENSE;

STORE con_rank INTO 'salida/especie_profit' USING PigStorage('\t');
