from storey import Filter, JoinWithV3IOTable, JoinWithHttp, Map, Reduce, Source, NeedsV3ioAccess, HttpRequest, build_flow, WriteToV3IOStream

import aiohttp
import asyncio
import json
import time


class SetupKvTable(NeedsV3ioAccess):
    async def setup(self, table_path):
        connector = aiohttp.TCPConnector()
        client_session = aiohttp.ClientSession(connector=connector)
        for i in range(1, 10):
            request_body = json.dumps({'Item': {'secret': {'N': f'{10 - i}'}}})
            response = await client_session.request(
                'PUT', f'{self._webapi_url}/{table_path}/{i}', headers=self._put_item_headers, data=request_body, ssl=False)
            assert response.status == 200, f'Bad response {response} to request {request_body}'


class SetupStream(NeedsV3ioAccess):
    async def setup(self, stream_path):
        connector = aiohttp.TCPConnector()
        client_session = aiohttp.ClientSession(connector=connector)
        request_body = json.dumps({"ShardCount": 2, "RetentionPeriodHours": 1})
        response = await client_session.request(
            'POST', f'{self._webapi_url}/{stream_path}/', headers=self._create_stream_headers, data=request_body, ssl=False)
        assert response.status == 204, f'Bad response {response} to request {request_body}'


def test_join_with_v3io_table():
    table_path = f'bigdata/test_join_with_v3io_table/{int(time.time_ns() / 1000)}'
    asyncio.run(SetupKvTable().setup(table_path))
    controller = build_flow([
        Source(),
        Map(lambda x: x + 1),
        Filter(lambda x: x < 8),
        JoinWithV3IOTable(lambda x: x.element, lambda x, y: y['secret'], table_path),
        Reduce(0, lambda x, y: x + y)
    ]).run()
    for i in range(10):
        controller.emit(i)

    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 42


def test_join_with_http():
    controller = build_flow([
        Source(),
        Map(lambda x: x + 1),
        Filter(lambda x: x < 8),
        JoinWithHttp(lambda _: HttpRequest('GET', 'https://google.com', ''), lambda _, response: response.status),
        Reduce(0, lambda x, y: x + y)
    ]).run()
    for i in range(10):
        controller.emit(i)

    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 200 * 7


def test_write_to_v3io_stream():
    stream_path = f'bigdata/test_write_to_v3io_stream/{int(time.time_ns() / 1000)}/'
    asyncio.run(SetupStream().setup(stream_path))
    controller = build_flow([
        Source(),
        Map(lambda x: str(x)),
        WriteToV3IOStream(stream_path, partition_func=lambda event: str(int(event.element) % 2))
    ]).run()
    for i in range(10):
        controller.emit(i)

    controller.terminate()
    controller.await_termination()
