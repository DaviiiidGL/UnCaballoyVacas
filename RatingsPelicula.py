from mrjob.job import MRJob
from mrjob.step import MRStep


class ConteoRatingsPeliculas(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_ratings,
                   reducer=self.reducer_contar),
            MRStep(reducer=self.reducer_ordenar)
        ]

    def mapper_ratings(self, _, linea):
        valores = linea.strip().split("\t")

        if len(valores) == 4:
            id_pelicula = valores[1]
            yield id_pelicula, 1

    def reducer_contar(self, id_pelicula, conteos):
        total = sum(conteos)
        yield None, (total, id_pelicula)

    def reducer_ordenar(self, _, pares):
        ranking = sorted(pares, reverse=True)

        for conteo, id_pelicula in ranking:
            yield id_pelicula, conteo


if __name__ == "__main__":
    ConteoRatingsPeliculas.run()
