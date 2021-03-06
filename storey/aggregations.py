import asyncio
import copy
from datetime import datetime

from .aggregation_utils import is_raw_aggregate, get_virtual_aggregation_func, get_dependant_aggregates
from .dtypes import EmitEveryEvent, FixedWindows, EmitAfterPeriod, EmitAfterWindow, EmitAfterMaxEvent
from .flow import Flow, _termination_obj, Event

_default_emit_policy = EmitEveryEvent()


class AggregateByKey(Flow):
    def __init__(self, aggregates, table, key=None, emit_policy=_default_emit_policy, augmentation_fn=None):
        Flow.__init__(self)
        self._aggregates_store = AggregateStore(aggregates)
        self._table = table
        self._aggregates_metadata = aggregates

        self._emit_policy = emit_policy
        self._events_in_batch = {}
        self._emit_worker_running = False
        self._terminate_worker = False

        self._augmentation_fn = augmentation_fn
        if not augmentation_fn:
            def f(element, features):
                features.update(element)
                return features

            self._augmentation_fn = f

        self.key_extractor = None
        if key:
            if callable(key):
                self.key_extractor = key
            elif isinstance(key, str):
                self.key_extractor = lambda element: element[key]
            else:
                raise TypeError(f'key is expected to be either a callable or string but got {type(key)}')

    async def _do(self, event):
        if event == _termination_obj:
            self._terminate_worker = True
            return await self._do_downstream(_termination_obj)

        # check whether a background loop is needed, if so create start one
        if (not self._emit_worker_running) and \
                (isinstance(self._emit_policy, EmitAfterPeriod) or isinstance(self._emit_policy, EmitAfterWindow)):
            asyncio.get_running_loop().create_task(self._emit_worker())
            self._emit_worker_running = True

        element = event.body
        key = event.key
        if self.key_extractor:
            key = self.key_extractor(element)

        self._aggregates_store.aggregate(key, element, event.time)

        if isinstance(self._emit_policy, EmitEveryEvent):
            await self._emit_event(key, event)
        elif isinstance(self._emit_policy, EmitAfterMaxEvent):
            self._events_in_batch[key] = self._events_in_batch.get(key, 0) + 1
            if self._events_in_batch[key] == self._emit_policy.max_events:
                await self._emit_event(key, event)
                self._events_in_batch[key] = 0

    # Emit a single event for the requested key
    async def _emit_event(self, key, event):
        features = self._aggregates_store.get_features(key, event.time)
        features = self._augmentation_fn(event.body, features)
        new_event = copy.copy(event)
        new_event.key = key
        new_event.body = features
        await self._do_downstream(new_event)

    # Emit multiple events for every key in the store with the current time
    async def _emit_all_events(self, timestamp):
        for key in self._aggregates_store.get_keys():
            await self._emit_event(key, Event({'key': key, 'time': timestamp}, key, timestamp, None))

    async def _emit_worker(self):
        if isinstance(self._emit_policy, EmitAfterPeriod):
            seconds_to_sleep_between_emits = self._aggregates_metadata[0].windows.period_millis / 1000
        elif isinstance(self._emit_policy, EmitAfterWindow):
            seconds_to_sleep_between_emits = self._aggregates_metadata[0].windows.windows[0][0] / 1000
        else:
            raise TypeError(f'Emit policy "{type(self._emit_policy)}" is not supported')

        current_time = datetime.now().timestamp()
        next_emit_time = int(
            current_time / seconds_to_sleep_between_emits) * seconds_to_sleep_between_emits + seconds_to_sleep_between_emits

        while not self._terminate_worker:
            current_time = datetime.now().timestamp()
            next_sleep_interval = next_emit_time - current_time + self._emit_policy.delay_in_seconds
            if next_sleep_interval > 0:
                await asyncio.sleep(next_sleep_interval)
            await self._emit_all_events(next_emit_time * 1000)
            next_emit_time = next_emit_time + seconds_to_sleep_between_emits


class AggregatedStoreElement:
    def __init__(self, key, aggregates, base_time):
        self.aggregation_buckets = {}
        self.key = key
        self.aggregates = aggregates

        # Add all raw aggregates, including aggregates not explicitly requested.
        for aggregation_metadata in aggregates:
            for aggr in aggregation_metadata.get_all_raw_aggregates():
                self.aggregation_buckets[f'{aggregation_metadata.name}_{aggr}'] = \
                    AggregationBuckets(aggregation_metadata.name, aggr, aggregation_metadata.windows, base_time,
                                       aggregation_metadata.max_value)

        # Add all virtual aggregates
        for aggregation_metadata in aggregates:
            for aggr in aggregation_metadata.aggregations:
                if not is_raw_aggregate(aggr):
                    dependant_aggregate_names = get_dependant_aggregates(aggr)
                    dependant_buckets = []
                    for dep in dependant_aggregate_names:
                        dependant_buckets.append(self.aggregation_buckets[f'{aggregation_metadata.name}_{dep}'])
                    self.aggregation_buckets[f'{aggregation_metadata.name}_{aggr}'] = \
                        VirtualAggregationBuckets(aggregation_metadata.name, aggr, aggregation_metadata.windows,
                                                  base_time, dependant_buckets)

    def aggregate(self, data, timestamp):
        # add a new point and aggregate
        for aggregation_metadata in self.aggregates:
            if aggregation_metadata.should_aggregate(data):
                curr_value = aggregation_metadata.value_extractor(data)
                for aggr in aggregation_metadata.get_all_raw_aggregates():
                    self.aggregation_buckets[f'{aggregation_metadata.name}_{aggr}'].aggregate(timestamp, curr_value)

    def get_features(self, timestamp):
        result = {}
        for aggregation_bucket in self.aggregation_buckets.values():
            result.update(aggregation_bucket.get_features(timestamp))

        return result


class AggregateStore:
    def __init__(self, aggregates):
        self.cache = {}
        self.aggregates = aggregates

    def __iter__(self):
        return iter(self.cache.items())

    def aggregate(self, key, data, timestamp):
        if isinstance(timestamp, datetime):
            timestamp = timestamp.timestamp() * 1000

        if key not in self.cache:
            self.cache[key] = AggregatedStoreElement(key, self.aggregates, timestamp)

        self.cache[key].aggregate(data, timestamp)

    def get_features(self, key, timestamp):
        if isinstance(timestamp, datetime):
            timestamp = timestamp.timestamp() * 1000

        return self.cache[key].get_features(timestamp)

    def get_keys(self):
        return self.cache.keys()


class AggregationBuckets:
    def __init__(self, name, aggregation, window, base_time, max_value):
        self.name = name
        self.aggregation = aggregation
        self.window = window
        self.max_value = max_value
        self.buckets = []
        self.first_bucket_start_time = self.window.get_window_start_time_by_time(base_time)
        self.last_bucket_start_time = \
            self.first_bucket_start_time + (window.total_number_of_buckets - 1) * window.period_millis

        self.initialize_column()

    def initialize_column(self):
        self.buckets = []

        for _ in range(self.window.total_number_of_buckets):
            self.buckets.append(AggregationValue(self.aggregation, self.max_value))

    def get_or_advance_bucket_index_by_timestamp(self, timestamp):
        if timestamp < self.last_bucket_start_time + self.window.period_millis:
            bucket_index = int((timestamp - self.first_bucket_start_time) / self.window.period_millis)
            return bucket_index
        else:
            self.advance_window_period(timestamp)
            return self.window.total_number_of_buckets - 1  # return last index

    #  Get the index of the bucket corresponding to the requested timestamp
    #  Note: This method can return indexes outside the 'buckets' array
    def get_bucket_index_by_timestamp(self, timestamp):
        bucket_index = int((timestamp - self.first_bucket_start_time) / self.window.period_millis)
        return bucket_index

    def get_nearest_window_index_by_timestamp(self, timestamp, window_millis):
        bucket_index = int((timestamp - self.first_bucket_start_time) / window_millis)
        return bucket_index

    def advance_window_period(self, advance_to):
        desired_bucket_index = int((advance_to - self.first_bucket_start_time) / self.window.period_millis)
        buckets_to_advance = desired_bucket_index - (self.window.total_number_of_buckets - 1)

        if buckets_to_advance > 0:
            if buckets_to_advance > self.window.total_number_of_buckets:
                self.initialize_column()
            else:
                self.buckets = self.buckets[buckets_to_advance:]
                for _ in range(buckets_to_advance):
                    self.buckets.extend([AggregationValue(self.aggregation, self.max_value)])

            self.first_bucket_start_time = \
                self.first_bucket_start_time + buckets_to_advance * self.window.period_millis
            self.last_bucket_start_time = \
                self.last_bucket_start_time + buckets_to_advance * self.window.period_millis

    def aggregate(self, timestamp, value):
        index = self.get_or_advance_bucket_index_by_timestamp(timestamp)
        self.buckets[index].aggregate(timestamp, value)

    def get_aggregation_for_aggregation(self):
        if self.aggregation == 'count':
            return 'sum'
        return self.aggregation

    def get_features(self, timestamp):
        result = {}

        current_time_bucket_index = self.get_bucket_index_by_timestamp(timestamp)
        if isinstance(self.window, FixedWindows):
            current_time_bucket_index = self.get_bucket_index_by_timestamp(self.window.round_up_time_to_window(timestamp) - 1)

        aggregated_value = AggregationValue(self.get_aggregation_for_aggregation())
        prev_windows_millis = 0
        for i in range(len(self.window.windows)):
            window_string = self.window.windows[i][1]
            window_millis = self.window.windows[i][0]

            # In case the current bucket is outside our time range just create a feature with the current aggregated
            # value
            if current_time_bucket_index < 0:
                result[f'{self.name}_{self.aggregation}_{window_string}'] = aggregated_value.get_value()

            number_of_buckets_backwards = int((window_millis - prev_windows_millis) / self.window.period_millis)
            last_bucket_to_aggregate = current_time_bucket_index - number_of_buckets_backwards + 1

            if last_bucket_to_aggregate < 0:
                last_bucket_to_aggregate = 0

            for bucket_index in range(current_time_bucket_index, last_bucket_to_aggregate - 1, -1):
                if bucket_index < len(self.buckets):
                    t, v = self.buckets[bucket_index].get_value()
                    aggregated_value.aggregate(t, v)

            # advance the time bucket, so that next iteration won't calculate the same buckets again
            current_time_bucket_index = last_bucket_to_aggregate - 1

            # create a feature for the current time window
            result[f'{self.name}_{self.aggregation}_{window_string}'] = aggregated_value.get_value()[1]
            prev_windows_millis = window_millis

        return result


class VirtualAggregationBuckets:
    def __init__(self, name, aggregation, window, base_time, args):
        self.name = name
        self.args = args
        self.aggregation = aggregation
        self.aggregation_func = get_virtual_aggregation_func(aggregation)
        self.window = window
        self.first_bucket_start_time = self.window.get_window_start_time_by_time(base_time)
        self.last_bucket_start_time = \
            self.first_bucket_start_time + (window.total_number_of_buckets - 1) * window.period_millis

    def aggregate(self, timestamp, value):
        pass

    def get_features(self, timestamp):
        result = {}

        args_results = [list(bucket.get_features(timestamp).values()) for bucket in self.args]

        for i in range(len(args_results[0])):
            window_string = self.window.windows[i][1]
            current_args = []
            for window_result in args_results:
                current_args.append(window_result[i])

            result[f'{self.name}_{self.aggregation}_{window_string}'] = self.aggregation_func(current_args)
        return result


class FieldAggregator:
    def __init__(self, name, field, aggr, windows, aggr_filter=None, max_value=None):
        if aggr_filter is not None and not callable(aggr_filter):
            raise TypeError(f'aggr_filter expected to be callable, got {type(aggr_filter)}')

        if callable(field):
            self.value_extractor = field
        elif isinstance(field, str):
            self.value_extractor = lambda element: element[field]
        else:
            raise TypeError(f'field is expected to be either a callable or string but got {type(field)}')

        self.name = name
        self.aggregations = aggr
        self.windows = windows
        self.aggr_filter = aggr_filter
        self.max_value = max_value

    def get_all_raw_aggregates(self):
        raw_aggregates = {}

        for aggregate in self.aggregations:
            if is_raw_aggregate(aggregate):
                raw_aggregates[aggregate] = True
            else:
                for dependant_aggr in get_dependant_aggregates(aggregate):
                    raw_aggregates[dependant_aggr] = True

        return raw_aggregates.keys()

    def should_aggregate(self, element):
        if not self.aggr_filter:
            return True

        return self.aggr_filter(element)


class AggregationValue:
    def __init__(self, aggregation, max_value=None):
        self.aggregation = aggregation

        self._value = self.get_default_value()
        self._first_time = datetime.max
        self._last_time = datetime.max
        self._max_value = max_value

    def aggregate(self, time, value):
        if self.aggregation == 'min':
            self._set_value(min(self._value, value))
        elif self.aggregation == 'max':
            self._set_value(max(self._value, value))
        elif self.aggregation == 'sum':
            self._set_value(self._value + value)
        elif self.aggregation == 'count':
            self._set_value(self._value + 1)
        elif self.aggregation == 'last' and time > self._last_time:
            self._set_value(value)
            self._last_time = time
        elif self.aggregation == 'first' and time < self._first_time:
            self._set_value(value)
            self._first_time = time

    def _set_value(self, value):
        if self._max_value:
            self._value = min(self._max_value, value)
        else:
            self._value = value

    def get_default_value(self):
        if self.aggregation == 'max':
            return float('-inf')
        elif self.aggregation == 'min':
            return float('inf')
        elif self.aggregation == 'first' or self.aggregation == 'last':
            return None
        else:
            return 0

    def get_value(self):
        value_time = self._last_time
        if self.aggregation == 'first':
            value_time = self._first_time
        return value_time, self._value
