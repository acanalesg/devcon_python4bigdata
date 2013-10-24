
def averageDurationMapper(key, value):
    """
    MAPPER
    Reads an input text file with user information and
    gives the tuple userid-duration.
        Input (txt file):
            key: NumberOfLine,
            value: userId|duration
        Output format:
            key: userId
            value: duration
    """

    (userId, duration) = value.split("|")
    yield userId, int(duration)


def averageDurationCombiner(key, values):
    """
    COMBINER
    Receives the list of durations of a calls of a client and
    gives as output the sum of these durations and the number
    of calls received
        Input:
            key: userId
            value: duration
        Output:
            key: userId
            value: duration, num_of_calls
    """

    #
    values_list = list(values)
    yield key, (sum(values_list), len(values_list))


def averageDurationReducer(key, values):
    """
    REDUCER
    Receives the list of average durations of a piece of calls
    of a client and the number of this piece of calls, and gives
    as output the average duration of all calls of the client
        Input:
            key: userId
            value: duration, num_of_calls
        Output:
            key: userId
    """

    sum = 0.0
    numCalls = 0

    for value in values:
        sum += value[0]
        numCalls += value[1]

    yield str(key) + '|' + str(float(sum/numCalls)), ''


def runner(job):
    opts = [("inputformat", "text"), ("outputformat", "text"), ]
    out = job.additer(averageDurationMapper, averageDurationReducer, combiner=averageDurationCombiner, opts=opts)

if __name__ == "__main__":
    from dumbo import main
    main(runner)





