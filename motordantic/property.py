__all__ = (
    'cached_classproperty',
    'classproperty',
)


class cached_classproperty(classmethod):
    def __init__(self, fget):
        self.obj = {}
        self.fget = fget

    def __get__(self, owner, cls):
        if cls in self.obj:
            return self.obj[cls]
        self.obj[cls] = self.fget(cls)
        return self.obj[cls]


class classproperty(classmethod):
    def __init__(self, method=None):
        self.fget = method

    def __get__(self, instance, cls=None):
        return self.fget(cls)  # type: ignore

    def getter(self, method):
        self.fget = method
        return self
