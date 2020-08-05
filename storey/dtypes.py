from datetime import datetime
from enum import Enum

from .utils import parse_duration, bucketPerWindow


class WindowBase:
    def __init__(self, window, period, window_str):
        self.window_millis = window
        self.period_millis = period
        self.window_str = window_str


class FixedWindow(WindowBase):
    def __init__(self, window):
        window_millis = parse_duration(window)
        WindowBase.__init__(self, window_millis, window_millis / bucketPerWindow, window)

    def get_total_number_of_buckets(self):
        return bucketPerWindow * 2

    def get_window_start_time(self):
        return self.get_current_window()

    def get_current_window(self):
        return int((datetime.now().timestamp() * 1000) / self.window_millis) * self.window_millis

    def get_current_period(self):
        return int((datetime.now().timestamp() * 1000) / self.period_millis) * self.period_millis


class SlidingWindow(WindowBase):
    def __init__(self, window, period):
        window_millis, period_millis = parse_duration(window), parse_duration(period)
        if not window_millis % period_millis == 0:
            raise Exception('period must be a divider of the window')

        WindowBase.__init__(self, window_millis, period_millis, window)

    def get_total_number_of_buckets(self):
        return int(self.window_millis / self.period_millis)

    def get_window_start_time(self):
        return datetime.now().timestamp() * 1000


class WindowsBase:
    def __init__(self, period, max_window, smallest_window, windows_str):
        self.max_window_millis = max_window
        self.smallest_window_millis = smallest_window
        self.period_millis = period
        self.windows_str = windows_str
        self.windows_millis = [parse_duration(win) for win in windows_str]
        self.total_number_of_buckets = int(self.max_window_millis / self.period_millis)


class FixedWindows(WindowsBase):
    def __init__(self, windows):
        max_window_millis = parse_duration(windows[-1])
        smallest_window_millis = parse_duration(windows[0])
        WindowsBase.__init__(self, smallest_window_millis / bucketPerWindow,
                             max_window_millis, smallest_window_millis, windows)

    def get_window_start_time(self):
        return self.get_period_by_time(datetime.now().timestamp() * 1000)

    def get_current_window(self):
        return int((datetime.now().timestamp() * 1000) / self.smallest_window_millis) * self.smallest_window_millis

    def get_period_by_time(self, timestamp):
        return int(timestamp / self.period_millis) * self.period_millis

    def get_window_start_time_by_time(self, timestamp):
        return self.get_period_by_time(timestamp)


class SlidingWindows(WindowsBase):
    def __init__(self, windows, period=None):

        max_window_millis = parse_duration(windows[-1])
        smallest_window_millis = parse_duration(windows[0])

        if period:
            period_millis = parse_duration(period)

            # Verify the given period is a divider of the windows
            for window in windows:
                if not parse_duration(window) % period_millis == 0:
                    raise Exception(
                        f'period must be a divider of every window, but period {period} does not divide {window}')
        else:
            period_millis = smallest_window_millis / bucketPerWindow

        WindowsBase.__init__(self, period_millis, max_window_millis, smallest_window_millis, windows)

    def get_window_start_time_by_time(self, timestamp):
        return timestamp

    def get_window_start_time(self):
        return datetime.now().timestamp() * 1000


class EmissionType(Enum):
    All = 1
    Incremental = 2


class EmitBase:
    def __init__(self, emission_type=EmissionType.All):
        self.emission_type = emission_type


class EmitAfterPeriod(EmitBase):
    pass


class EmitAfterWindow(EmitBase):
    pass


class EmitAfterMaxEvent(EmitBase):
    def __init__(self, max_events, emission_type=EmissionType.All):
        self.max_events = max_events
        EmitBase.__init__(self, emission_type)


class EmitAfterDelay(EmitBase):
    def __init__(self, delay_in_seconds, emission_type=EmissionType.All):
        self.delay_in_seconds = delay_in_seconds
        EmitBase.__init__(self, emission_type)


class EmitEveryEvent(EmitBase):
    pass


class LateDataHandling(Enum):
    Nothing = 1
    Sort_before_emit = 2
