# Lista de proveedores a los que mas suministros se les pide

from mrjob.job import MRJob
from mrjob.step import MRStep

class ProveedorBreakdown(MRJob):
  def steps(self):
    return [
      MRStep(mapper=self.mapper_get_pedidos,
             reducer=self.reducer_count_pedidos),
        MRStep(reducer=self.reducer_orden)
    ]

  def mapper_get_pedidos(self, _, line):
    (pedidoID, proveedorID, fecha, valor, cantidad, estado) = line.split("\t")
    yield proveedorID, int(cantidad)
  
  def reducer_count_pedidos(self, key, values):
    yield None, (sum(values), key)

  def reducer_orden(self, _, pairs):
    for total, proveedor in sorted(pairs, reverse=True):
        yield proveedor, total 

if __name__ == '__main__': 
     ProveedorBreakdown.run()