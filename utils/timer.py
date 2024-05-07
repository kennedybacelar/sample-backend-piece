import time


class Timer:
    def __init__(self, name, callback=print, threshold_ms=0):
        self.name = name
        self.start_time = None
        self.end_time = None
        self.threshold_ms = threshold_ms
        self.callback = callback

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc, value, tb):
        self.end_time = time.perf_counter()
        delta_ms = (self.end_time - self.start_time) * 1000
        if delta_ms >= self.threshold_ms:
            self.callback(self.name, delta_ms)


class LogTimeIt:
    def __init__(self, name, logger, threshold_ms=1000):
        self.name = name
        self.start_time = None
        self.end_time = None
        self.threshold_ms = threshold_ms
        self.logger = logger

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc, value, tb):
        self.end_time = time.perf_counter()
        delta_ms = (self.end_time - self.start_time) * 1000
        if delta_ms >= self.threshold_ms:
            self.logger.warning(f"{self.name} took {delta_ms}ms")
        else:
            self.logger.debug(f"{self.name} took {delta_ms}ms")


def time_it_log(logger, threshold_ms=1000):
    def inner(fn):
        def wrapper(*args, **kwargs):
            with LogTimeIt(fn.__name__, logger, threshold_ms):
                ret = fn(*args, **kwargs)
            return ret

        return wrapper

    return inner
