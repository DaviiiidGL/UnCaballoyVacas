# Insumo Ganadero Más Utilizado

from mrjob.job import MRJob
from mrjob.step import MRStep

class InsumoGanaderoBreakdown(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_join, reducer=self.reducer_join), 
            MRStep(reducer=self.reducer_join2)
            MRStep(reducer=self.reducer_orden)
        ]

    def mapper_join(self, _, line):
        valores = line.split("\t")
        
        if len(valores) == 6:
            id_suministro = valores[3]
            yield id_suministro, ("T", 1)

        elif len(valores) == 5:
            id_tipo = valores[0]
            nombre = valores[1]
            categoria = valores[2]
            yield id_tipo, ("TS", nombre, categoria)

        elif len(valores) == 4:
            id_suministro = valores[0]
            id_tipo = valores[1]
            yield id_suministro, ("S", id_tipo)
        
    def reducer_join(self, key, values):
        count = 0
        id_tipo = None
        tiposum = None

        for v in values:
            if v[0] == "T":
                count += 1
            
            elif v[0] == "S":
                id_tipo = v[1]

            elif v[0] == "TS":
                tiposum = (v[1], v[2])
            
        if id_tipo and count > 0:  
            yield id_tipo, ("count", count)

        if tiposum:
            yield key, ("tipo", tiposum[1], tiposum[2])

    def reducer_join2(self, id_tipo, values):
        count = 0
        nombre = None
        categoria = None
        for v in values:
            if v[0] == "count":
                count += 1
            elif v[0] == "tipo":
                nombre = v[1]
                categoria = v[2]

        if categoria == "insumo_ganadero" and conteo > 0:
            yield None, (conteo, nombre)

    def reducer_orden(self, _, pairs):
        top = sorted(pairs, reverse=True)
        yield top[0][0], top[0][1]

if __name__ == '__main__': 
     InsumoGanaderoBreakdown.run()
