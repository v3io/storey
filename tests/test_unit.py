import datetime

import storey
from storey import windowed_store as ws
import time


def test_normal_flow():
    flow = storey.build_flow([
        storey.Source(),
        storey.Map(lambda x: x + 1),
        storey.JoinWithTable(
            # TODO: container name from configuration
            lambda x: x, lambda x, y: y['secret'], '/bigdata/gal'),
        storey.Map(aprint_store)
    ])

    start = time.monotonic()

    mat = flow.run()
    for _ in range(100):
        for i in range(10):
            mat.emit(i)
    mat.emit(None)

    end = time.monotonic()
    print(end - start)


async def aprint_store(store):
    cache = store.cache
    print('store: ')
    for elem in cache:
        print(
            elem, '-', cache[elem].features,
            f'start time - {cache[elem].first_bucket_start_time}')
    print()


def test_windowed_flow():
    flow = storey.build_flow([
        storey.Source(),
        ws.Window(
            ws.SlidingWindow('30s', '5s'), 'key', 'time',
            storey.EmitAfterPeriod()),
        storey.Map(aprint_store)
    ])

    start = time.monotonic()
    running_flow = flow.run()
    for i in range(32):
        data = {
            'key': f'{i % 4}',
            'time': datetime.datetime.now() + datetime.timedelta(minutes=i),
            'col1': i,
            'other_col': i * 2,
        }
        running_flow.emit(data)

    end = time.monotonic()
    print(end - start)

    time.sleep(12)
    running_flow.emit(None)
