import dumbo.mapredtest as mrt
import averageDuration


def averageDurationMapperTest():

    instance = mrt.MapDriver(averageDuration.averageDurationMapper)

    input_kvs = list()

    input_key1 = 1
    input_key2 = 2
    input_key3 = 3
    input_value1 = "user1|44"
    input_value2 = "user2|125"
    input_value3 = "user1|367"
    input_kvs.append((input_key1, input_value1))
    input_kvs.append((input_key2, input_value2))
    input_kvs.append((input_key3, input_value3))

    output_kvs = [("user1", 44), ("user2", 125), ("user1", 367)]

    instance.with_input(input_kvs).with_output(output_kvs).run()


def averageDurationCombinerTest():

    instance = mrt.ReduceDriver(averageDuration.averageDurationCombiner)

    input_kvs = list()
    input_key = "user"
    input_value1 = 44
    input_value2 = 125
    input_value3 = 367
    input_kvs.append((input_key, input_value1))
    input_kvs.append((input_key, input_value2))
    input_kvs.append((input_key, input_value3))

    output_kvs = [("user",(536, 3))]

    instance.with_input(input_kvs).with_output(output_kvs).run()


def averageDurationReducerTest():

    instance = mrt.ReduceDriver(averageDuration.averageDurationReducer)

    input_kvs = list()
    input_key = "user"
    input_value1 = (230, 3)
    input_value2 = (170, 1)
    input_kvs.append((input_key, input_value1))
    input_kvs.append((input_key, input_value2))

    output_kvs = [("user|100.0", '')]

    instance.with_input(input_kvs).with_output(output_kvs).run()


if __name__ == "__main__":
    averageDurationMapperTest()
    averageDurationCombinerTest()
    averageDurationReducerTest()





