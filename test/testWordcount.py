import dumbo.mapredtest as mrt
import wordcount


def wordcountTest():

    instance = mrt.MapReduceDriver(wordcount.wordCountMapper,
                                   wordcount.wordCountReducer)

    input_kvs = [(1, "solo se que no se nada")]

    output_kvs = [("solo", 1), ("se", 2), ("que", 1), ("no", 1), ("nada", 1)]

    instance.with_input(input_kvs).with_output(sorted(output_kvs)).run()


if __name__ == "__main__":
    wordcountTest()





