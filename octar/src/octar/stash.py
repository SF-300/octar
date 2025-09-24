import typing as t

from octar.base import ActorMessage, Envelope, Mailbox, MsgPriority


class LiveStash[M: ActorMessage](t.MutableSequence[M]):
    def __init__(
        self,
        check_is_processing: t.Callable[[], bool],
        mailbox: Mailbox[M],
        buffer: t.Sequence[M],
        msg_idx_iter: t.Iterator[int],
    ) -> None:
        self._check_is_processing = check_is_processing
        self._mailbox = mailbox
        self._buffer = list(buffer)
        self._msg_idx_iter = msg_idx_iter

    def __getitem__(self, index):
        return self._buffer[index]

    def __len__(self):
        return len(self._buffer)

    def __setitem__(self, index, value):
        if not self._check_is_processing():
            raise RuntimeError("Cannot modify LiveStash while actor is not processing")
        self._buffer[index] = value

    def __delitem__(self, index):
        if not self._check_is_processing():
            raise RuntimeError("Cannot modify LiveStash while actor is not processing")
        del self._buffer[index]

    def insert(self, index, value):
        if not self._check_is_processing():
            raise RuntimeError("Cannot modify LiveStash while actor is not processing")
        self._buffer.insert(index, value)

    def rindex(self, value: M) -> int:
        self.reverse()
        i = self.index(value)
        self.reverse()
        return len(self) - i - 1

    def unstash_all(self) -> None:
        if not self._check_is_processing():
            raise RuntimeError("Cannot modify LiveStash while actor is not processing")
        while self._buffer:
            msg = self._buffer.pop(0)
            self._mailbox.put_nowait(Envelope(MsgPriority.HIGH, next(self._msg_idx_iter), msg))

    def __call__(self, msg: M) -> None:
        self.append(msg)