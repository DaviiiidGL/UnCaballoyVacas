# Zonas Ordenadas Por Cantidad de Animales
# Pide ANIMALES y EDIFICIOS

from mrjob.job import MRJob
from mrjob.step import MRStep


class ZonasAnimalesBreakdown(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_join, reducer=self.reducer_join),
            MRStep(reducer=self.reducer_suma_zona),
            MRStep(reducer=self.reducer_orden)
        ]

    def mapper_join(self, _, line):
        valores = line.strip().split("\t")

        if valores[0].startswith("AN"):
            id_edificio = valores[1]
            yield id_edificio, ("A", 1)

        elif valores[0].startswith("ED"):
            id_zona = valores[1]
            yield valores[0], ("E", id_zona)

    def reducer_join(self, id_edificio, values):
        count = 0
        id_zona = None

        for v in values:
            if v[0] == "A":
                count += 1
            elif v[0] == "E":
                id_zona = v[1]

        if id_zona and count > 0:
            yield id_zona, count

    def reducer_suma_zona(self, id_zona, counts):
        yield None, (sum(counts), id_zona)

    def reducer_orden(self, _, pairs):
        for count, zona in sorted(pairs, reverse=True):
            yield zona, count


if __name__ == '__main__':
    ZonasAnimalesBreakdown.run()
