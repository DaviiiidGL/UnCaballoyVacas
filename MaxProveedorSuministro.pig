proveedores = LOAD 'PROVEEDOR.data' USING PigStorage('\t') AS (id_proveedor:chararray, nombre:chararray);

pedidos = LOAD 'PEDIDO.data' USING PigStorage('\t') AS (id_pedido:chararray, id_proveedor:chararray, fecha:chararray, precio:float, cantidad:int, estado:chararray);

PedidosyProveedores = JOIN proveedores BY id_proveedor, pedidos BY id_proveedor;

CantidadPedidoProveedor = FOREACH PedidosyProveedores GENERATE proveedores::id_proveedor AS id_proveedor, proveedores::nombre AS nombre, pedidos::cantidad AS cantidad;

PedidosPorProveedor = GROUP CantidadPedidoProveedor BY (id_proveedor, nombre);

totales = FOREACH PedidosPorProveedor GENERATE FLATTEN(group) AS (id_proveedor, nombre), SUM(CantidadPedidoProveedor.cantidad) AS total;

orden = RANK totales BY total DESC DENSE;

top10 = FILTER orden BY rank_totales <= 10;

STORE top10 INTO 'salida/top_proveedores' USING PigStorage('\t');
