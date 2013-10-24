import operator


def int_or_zero(val):
    try:
        return int(val)
    except:
        return 0


def parse_disasters(key, value):
    toks = value.split('\t')

    # (country, type), (killed, cost, affected)
    yield (toks[2], toks[4]), operator.itemgetter(7, 8, 9)(toks)


class ReduceLkp:
    """Mapper to parse disasters and look-up country index"""
    def __init__(self):    # initialize reducer (cache hdi)
        self.loadHdi()
        self.loadContinents()

    def __call__(self, key, values):    # function invoke for each record in disasters
        country_hdi = self.hdi.get(key[0])
        continent = self.continents.get(key[0])
        killed = 0
        cost = 0
        affected = 0
        if country_hdi:   # if found
            for v in values:
                killed += int_or_zero(v[0])
                cost += int_or_zero(v[1])
                affected += int_or_zero(v[2])
            yield ",".join((str(s) for s in key + (continent,) + country_hdi + (killed, cost, affected, ""))), ""

    def loadHdi(self):
        # Read HDI file and store in dict
        file = open('hdi.tsv', 'r')
        self.hdi = {}
        file.next()  # Remove header
        for l in file:
            tokens = l.strip().split('\t')
            # { country : (rank, hdi_2010) }
            self.hdi[tokens[1]] = operator.itemgetter(0, 10)(tokens)

    def loadContinents(self):
        # Read HDI file and store in dict
        file = open('country_continent.tsv', 'r')
        self.continents = {}
        for l in file:
            tokens = l.strip().split('\t')
            # { country : continent }
            self.continents[tokens[1]] = tokens[0]


def runner(job):
    opts = [("inputformat", "text"), ("outputformat", "text")]
    o1 = job.additer(parse_disasters, ReduceLkp, opts=opts)


if __name__ == "__main__":
    from dumbo import main
    main(runner)
