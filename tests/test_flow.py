from storey import flow


class TestException(Exception):
    pass


class RaiseEx:
    _counter = 0

    def __init__(self, raise_after):
        self._raise_after = raise_after

    def raise_ex(self, element):
        if self._counter == self._raise_after:
            raise TestException("test")
        self._counter += 1
        return element


def test_functional_flow():
    controller = flow.build_flow([
        flow.Source(),
        flow.Map(lambda x: x + 1),
        flow.Filter(lambda x: x < 3),
        flow.FlatMap(lambda x: [x, x * 10]),
        flow.Reduce(0, lambda acc, x: acc + x),
    ]).run()

    for _ in range(100):
        for i in range(10):
            controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert 3300 == termination_result


def test_error_flow():
    controller = flow.build_flow([
        flow.Source(),
        flow.Map(lambda x: x + 1),
        flow.Map(RaiseEx(500).raise_ex),
        flow.Reduce(0, lambda acc, x: acc + x),
    ]).run()

    try:
        for i in range(1000):
            controller.emit(i)
    except flow.FlowException as flow_ex:
        assert isinstance(flow_ex.__cause__, TestException)


def test_broadcast():
    controller = flow.build_flow([
        flow.Source(),
        flow.Map(lambda x: x + 1),
        flow.Filter(
            lambda x: x < 3, termination_result_fn=lambda x, y: x + y),
        [
            flow.Reduce(0, lambda acc, x: acc + x)
        ],
        [
            flow.Reduce(0, lambda acc, x: acc + x)
        ]
    ]).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert 6 == termination_result


def test_broadcast_complex():
    controller = flow.build_flow([
        flow.Source(),
        flow.Map(lambda x: x + 1),
        flow.Filter(
            lambda x: x < 3, termination_result_fn=lambda x, y: x + y
        ),
        [
            flow.Reduce(0, lambda acc, x: acc + x),
        ],
        [
            flow.Map(lambda x: x * 100),
            flow.Reduce(0, lambda acc, x: acc + x)
        ],
        [
            flow.Map(lambda x: x * 1000),
            flow.Reduce(0, lambda acc, x: acc + x)
        ]
    ]).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert 3303 == termination_result


# Same as test_broadcast_complex but without using build_flow
def test_broadcast_complex_no_sugar(self):
    source = flow.Source()
    filter = flow.Filter(
        lambda x: x < 3, termination_result_fn=lambda x, y: x + y)
    source.to(flow.Map(lambda x: x + 1)).to(filter)
    filter.to(flow.Reduce(0, lambda acc, x: acc + x), )
    filter.to(flow.Map(lambda x: x * 100)).to(
            flow.Reduce(0, lambda acc, x: acc + x))
    filter.to(flow.Map(lambda x: x * 1000)).to(
            flow.Reduce(0, lambda acc, x: acc + x))
    controller = source.run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert 3303 == termination_result
