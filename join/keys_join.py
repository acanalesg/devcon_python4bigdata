from dumbo.lib import JoinReducer, MultiMapper
from dumbo.decor import primary, secondary
import operator

def parse_disasters(key, value):
    toks = value.split('\t')

    # country, (start, type, subtype, killed, cost, affected)
    yield toks[2], operator.itemgetter(0, 4, 5,  7, 8, 9)(toks) 


def parse_hdi(key, value):
    toks = value.split('\t')

    # country, (rank, hdi_1980, hdi_1990, hdi_2000, hdi_2010)
    yield toks[1], operator.itemgetter(0, 2, 3, 4, 10)(toks)


class Reducer(JoinReducer):
    def primary(self, key, values):
        self.hdi_data = values.next()

    def secondary(self, key, values):
        for v in values:
            yield key, (self.hdi_data, v)


def runner(job):
    multimap = MultiMapper()
    multimap.add("hdi", primary(parse_hdi))
    multimap.add("disasters", secondary(parse_disasters))

    opts = [("inputformat", "text"), ("outputformat", "text"), ]
    o1 = job.additer(multimap, Reducer, opts=opts)

if __name__ == "__main__":
    from dumbo import main
    main(runner)
