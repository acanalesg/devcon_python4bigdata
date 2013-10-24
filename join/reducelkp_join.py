import operator


def parse_disasters(key, value):
    toks = value.split('\t')

    # country, (start, type, subtype, killed, cost, affected)
    yield toks[2], operator.itemgetter(0, 4, 5,  7, 8, 9)(toks)


class ReduceLkp:
    """Mapper to parse disasters and look-up country index"""
    def __init__(self):    # initialize reducer (cache hdi)
        self.loadHdi()

    def __call__(self, key, values):    # function invoke for each record in disasters
        country_hdi = self.hdi.get(key)
        if country_hdi:   # if found
            for v in values:
                # country, [rank_hdi, hdi_1980, hdi_1990, hdi_2000, hdi_2010,
                #           dis_start, type, subtype, killed, cost, affected]
                yield key, country_hdi + v

    def loadHdi(self):
        # Read HDI file and store in dict
        file = open('hdi.tsv', 'r')
        self.hdi = {}
        for l in file:
            tokens = l.strip().split('\t')
            # { country : (rank, hdi_1980, hdi_1990, hdi_2000, hdi_2010) }
            self.hdi[tokens[1]] = operator.itemgetter(0, 2, 3, 4, 10)(tokens)


def runner(job):
    opts = [("inputformat", "text"), ("outputformat", "text")]
    o1 = job.additer(parse_disasters, ReduceLkp, opts=opts)  # No reducer needed

if __name__ == "__main__":
    from dumbo import main
    main(runner)
