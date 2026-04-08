# Lista de proveedores a los que mas suministros se les pide
# Necesita de Pedido.data y Proveedor.data

from mrjob.job import MRJob
from mrjob.step import MRStep


class MaxProveedorSuministro(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_join,
                   reducer=self.reducer_join),
            MRStep(reducer=self.reducer_orden)
        ]

    def mapper_join(self, _, line):
        valores = line.strip().split("\t")

        if len(valores) == 2:
            id_proveedor = valores[0]
            nombre = valores[1]
            yield id_proveedor, ("P", nombre)

        elif len(valores) == 6:
            id_proveedor = valores[1]
            cantidad = int(valores[4])
            yield id_proveedor, ("T", cantidad)

    def reducer_join(self, id_proveedor, values):
        nombre = None
        total = 0

        for v in values:
            if v[0] == "P":
                nombre = v[1]
            elif v[0] == "T":
                total += v[1]

        if nombre and total > 0:
            yield None, (total, id_proveedor, nombre)

    def reducer_orden(self, _, triples):
        top10 = sorted(triples, reverse=True)[:10]
        rank = 1
        for total, id_proveedor, nombre in top10:
            yield rank, (id_proveedor, nombre, total)
            rank += 1


if __name__ == '__main__':
    MaxProveedorSuministro.run()