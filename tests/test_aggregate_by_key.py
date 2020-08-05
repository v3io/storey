from datetime import datetime, timedelta

from storey import build_flow, Source, Filter, Reduce, Map
from storey.aggregations import AggregateByKey, FieldAggregator
from storey.dtypes import SlidingWindows


def append_return(lst, x):
    lst.append(x)
    return lst


def validate_window(expected, window):
    for elem in window:
        key = elem[0]
        data = elem[1]
        for column in data.features:
            index = 0
            for bucket in data.features[column]:
                if len(bucket.data) > 0:
                    assert expected[key][index] == bucket.data

                index = index + 1


def to_millis(t):
    return t.timestamp() * 1000


def test_simple_aggregation_flow():
    controller = build_flow([
        Source(),
        # Filter(lambda x: x['col1'] > 3),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m'), '10m')],
                       'table'),
        Map(lambda x: print(x)),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    base_time = datetime.now() - timedelta(minutes=15)

    for i in range(10):
        data = {'col1': i}
        controller.emit(data, 'tal', base_time + timedelta(minutes=i))

    controller.terminate()
    aggregates_list = controller.await_termination()


def test_multiple_keys_aggregation_flow():
    controller = build_flow([
        Source(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m'), '10m')],
                       'table'),
        Map(lambda x: print(x)),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    base_time = datetime.now() - timedelta(minutes=15)

    for i in range(10):
        data = {'col1': i}
        controller.emit(data, f'{i % 2}', base_time + timedelta(minutes=i))

    controller.terminate()
    aggregates_list = controller.await_termination()


def test_aggregations_with_filters_flow():
    controller = build_flow([
        Source(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m'), '10m',
                                        aggr_filter=lambda element: element['is_valid'] == 0)],
                       'table'),
        Map(lambda x: print(x)),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    base_time = datetime.now() - timedelta(minutes=15)

    for i in range(10):
        data = {'col1': i, 'is_valid': i % 2}
        controller.emit(data, 'tal', base_time + timedelta(minutes=i))

    controller.terminate()
    aggregates_list = controller.await_termination()
    pass


def test_aggregations_with_max_values_flow():
    pass
