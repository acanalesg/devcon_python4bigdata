def wordCountMapper(key, value):

    words = value.split(" ")
    for word in words:
        yield word, 1


def wordCountReducer(key, values):

    yield key, sum(values)


def runner(job):
    opts = [("inputformat", "text"), ("outputformat", "text"), ]
    o1 = job.additer(wordCountMapper, wordCountReducer, opts=opts)

if __name__ == "__main__":
    from dumbo import main
    main(runner)





