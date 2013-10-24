import operator


class MapJoin:    # map is now a class, and initializes loading HDI
    """Mapper to parse disasters and look-up country index"""
    def __init__(self):    # initialize mapper (cache hdi)
        self.loadHdi()

    def __call__(self, key, value):    # function invoke for each record in disasters
        toks = value.split('\t')
        country = toks[2]
        country_hdi = self.hdi.get(country)
        if country_hdi:   # if found
            # country, [rank_hdi, hdi_1980, hdi_1990, hdi_2000, hdi_2010,
            #           dis_start, type, subtype, killed, cost, affected]
            yield country, country_hdi + operator.itemgetter(0, 4, 5,  7, 8, 9)(toks)

    def loadHdi(self):
        # Read HDI file and store in dict
        file = open('hdi.tsv', 'r')
        self.hdi = {}
        for l in file:
            tokens = l.strip().split('\t')
            # { country : (rank, hdi_1980, hdi_1990, hdi_2000, hdi_2010) }
            self.hdi[tokens[1]] = operator.itemgetter(0, 2, 3, 4, 10)(tokens)


def runner(job):
    opts = [("inputformat", "text"), ("outputformat", "text"), ("numreducetasks", "0")]
    o1 = job.additer(MapJoin, opts=opts)  # No reducer needed

if __name__ == "__main__":
    from dumbo import main
    main(runner)
