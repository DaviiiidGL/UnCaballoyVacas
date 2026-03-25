# Especie que mas ventas netas da

from mrjob.job import MRJob
from mrjob.step import MRStep
from operator import itemgetter

class EspecieProfitBreakdown(MRJob):
    
    def steps(self):
        return[
            MRStep(mapper=self.mapper_read, reducer=self.reducer_joinTarea),
            MRStep(mapper=self.mapper_pass, reducer=self.reducer_joinSuministro),
            MRStep(mapper=self.mapper_pass, reducer=self.reducer_joinTipoSuministro),
            MRStep(reducer=self.reducer_orden)
        ]

    def mapper_read(self, _, line):
        valores = line.split("\t")

        if valores[0].startswith("TG"):
            yield valores[2], ("T", valores[3])
        elif valores[0].startswith("AN"):
            yield valores[0], ("A", valores[3])
        elif valores[0].startswith("SU"):
            yield valores[0], ("S", valores[1])
        elif valores[0].startswith("TS"):
            yield valores[0], ("TS", valores[3])

    def reducer_joinTarea(self, key, values):
        tareas = []
        especie = id_tipoS = precio = None

        for v in values:
            if v[0] == "T": tareas.append(v[1])
            elif v[0] == "A": especie = v[1]
            elif v[0] == "S": id_tipoS = v[1]
            elif v[0] == "TS": precio = float(v[1])

        if tareas and especie:
            for id_S in tareas:
                yield id_S, ("Readyy", especie)
        if id_tipoS:
            yield key, ("S", id_tipoS)
        if precio is not None:
            yield key, ("TS", precio)

    def mapper_pass(self, key, value):
        yield key, value

    def reducer_joinSuministro(self, key, values):
        especies = []
        id_tipoS = precio = None
        
        for v in values:
            if v[0] == "Readyy": especies.append(v[1])
            elif v[0] == "S": id_tipoS = v[1]
            elif v[0] == "TS": precio = float(v[1])

        if especies and id_tipoS:
            for especie in especies:
                yield id_tipoS, ("Readyy2", especie)
        if precio is not None:
            yield key, ("TS", precio)

    def reducer_joinTipoSuministro(self, _, values):
        especies = []
        precio = None

        for v in values:
            if v[0] == "Readyy2": especies.append(v[1])
            elif v[0] == "TS": precio = float(v[1])

        if especies and precio is not None:
            for especie in especies:
                yield None, (precio, especie)
    
    def reducer_orden(self, _, pairs):
        total = {}

        for profit, especie in pairs:
            total[especie] = total.get(especie, 0) + profit

        total_ordenado = sorted(total.items(), key=itemgetter(1), reverse=True)

        for especie, totalF in total_ordenado:
            yield especie, round(totalF, 2)

        