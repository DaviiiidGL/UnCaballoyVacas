tarea = LOAD 'TAREA_GANADERIA.data' USING PigStorage('\t') AS (id_tarea:chararray, id_dron:chararray, id_animal:chararray, id_suministro:chararray, fecha:chararray, accio:chararray);

tipo = LOAD 'TIPO_SUMINISTRO.data' USING PigStorage('\t') AS (id_tipo:chararray, nombre:chararray, categoria:chararray, precio:float, cantidad:int);

suministro = LOAD 'SUMINISTRO.data' USING PigStorage('\t') AS (id_suministro:chararray, id_tipo:chararray, fecha_vencimiento:chararray, fecha_ingreso:chararray);

tareas = FOREACH tarea GENERATE id_suministro;

tareas_agrup = GROUP tareas BY id_suministro;

conteo = FOREACH tareas_agrup GENERATE group AS id_suministro, COUNT(tareas) AS count;

join1 = JOIN conteo BY id_suministro, suministro BY id_suministro;

tipojoin = FOREACH join1 GENERATE suministro::id_tipo AS id_tipo, conteo::count AS count;

tipo_agrup = GROUP tipojoin BY id_tipo;

totales_tipo = FOREACH tipo_agrup GENERATE group AS id_tipo, SUM(tipojoin.count) AS total;

join2 = JOIN totales_tipo BY id_tipo, tipo BY id_tipo;

nombrejoin = FOREACH join2 GENERATE tipo::nombre AS nombre, tipo::categoria AS categoria, totales_tipo::total AS total;

soloGanaderia = FILTER nombrejoin BY categoria == 'insumo_ganadero';

con_rank = RANK soloGanaderia BY total DESC DENSE;

top1 = FILTER con_rank BY rank_soloGanaderia <= 1;

STORE top1 INTO 'salida/insumo_ganadero_top' USING PigStorage('\t');
