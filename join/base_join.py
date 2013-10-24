import operator


def mapper(key, value):
    if key == 0:   # ignore header lines
        return

    toks = value.split('\t')

    if len(toks) == 12:     # disaster
        # country, ('Disaster', start, type, subtype, killed, cost, affected)
        yield toks[2], ('Disaster',) + operator.itemgetter(0, 4, 5,  7, 8, 9)(toks)

    elif len(toks) == 14:   # hdi
        # country, ('hdi', rank, hdi_1980, hdi_1990, hdi_2000, hdi_2010)
        yield toks[1], ('hdi',) + operator.itemgetter(0, 2, 3, 4, 10)(toks)


def reducer(key, values):
    # containers to split different types of values
    disasters = list()
    hdi = list()

    # loop on values, and send them to proper container
    for v in values:
        if v[0] == 'Disaster':
            disasters.append(v)
        else:
            hdi.append(v)

    # Do the join
    if len(hdi) == 1:
        for d in disasters:
            yield key, (hdi[0], d)


def runner(job):
    opts = [("inputformat", "text"), ("outputformat", "text"), ]
    o1 = job.additer(mapper, reducer, opts=opts)

if __name__ == "__main__":
    from dumbo import main
    main(runner)
