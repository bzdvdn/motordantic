from typing import TYPE_CHECKING

from contextlib import ContextDecorator

if TYPE_CHECKING:
    from .manager import ODMManager

__all__ = ('Session', 'SessionSync')


class Session(ContextDecorator):
    __slots__ = ('_session', 'odm_manager')

    def __init__(self, odm_manager: 'ODMManager'):
        self.odm_manager = odm_manager

    async def __aenter__(self):
        self._session = await self.odm_manager._start_session()
        return self._session

    async def __aexit__(self, *args, **kwargs):
        await self._session.end_session()  # type: ignore

    async def start_stransaction(
        self,
        read_concern=None,
        write_concern=None,
        read_preference=None,
        max_commit_time_ms=None,
    ):
        # TODO tranaction login with rs
        return self._session.start_transaction(
            read_preference=read_preference,
            write_concern=write_concern,
            read_concern=read_concern,
            max_commit_time_ms=max_commit_time_ms,
        )


class SessionSync(ContextDecorator):
    __slots__ = ('_session', 'odm_manager')

    def __init__(self, odm_manager: 'ODMManager'):
        self.odm_manager = odm_manager

    def __enter__(self):
        self._session = self.odm_manager._io_loop.run_until_complete(
            self.odm_manager._start_session()
        )
        return self._session

    def __exit__(self, *args, **kwargs):
        return self.odm_manager._io_loop.run_until_complete(self._session.end_session())  # type: ignore
