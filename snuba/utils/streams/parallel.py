import bisect
from typing import Iterator, MutableSequence, NamedTuple

Offset = int


class Epoch(NamedTuple):
    index: int
    offset: Offset


class OffsetTracker:
    def __init__(self) -> None:
        self.__index: MutableSequence[Epoch] = []
        self.__items: MutableSequence[bool] = []

    def __len__(self) -> int:
        """
        Return the total number of in-progress items.
        """
        if not self.__index:
            return 0

        return self.__items.count(False)

    def __iter__(self) -> Iterator[int]:
        if not self.__index:
            return

        epoch_index = 0
        for i, completed in enumerate(self.__items):
            try:
                if i >= self.__index[epoch_index + 1].index:
                    epoch_index = epoch_index + 1
            except IndexError:
                pass

            epoch = self.__index[epoch_index]
            offset = epoch.offset + (i - epoch.index)
            if not completed:
                yield offset

    def add(self, offset: Offset) -> None:
        """
        Add an offset to the set of in-progress items.
        """
        if not self.__index:
            # If this is the first write to the offset tracker, we need to
            # establish the initial epoch.
            self.__index.append(Epoch(0, offset))
        else:
            # If this is not the first write to the offset tracker, we need to
            # determine whether or not this offset value is the next value in a
            # gapless offset sequence, or if we should establish a new epoch.
            epoch = self.__index[-1]
            next_gapless_offset = epoch.offset + (len(self.__items) - epoch.index)
            if not offset == next_gapless_offset:
                if not offset > next_gapless_offset:
                    raise ValueError("offset sequence must be monotonic")
                self.__index.append(Epoch(len(self.__items), offset))

        self.__items.append(False)

    def remove(self, offset: Offset) -> None:
        """
        Remove an offset from the set of in-progress items.
        """
        epoch_index = bisect.bisect_right([e.offset for e in self.__index], offset) - 1
        if not epoch_index >= 0:
            raise KeyError(offset)  # item is out of range (too low)

        epoch = self.__index[epoch_index]

        index = epoch.index + (offset - epoch.offset)

        # Make sure this index doesn't run over into the next epoch. (This can
        # occur when trying to remove an item that was never added to the set.)
        try:
            next_epoch = self.__index[epoch_index + 1]
        except IndexError:
            pass
        else:
            if index >= next_epoch.index:
                raise KeyError(offset)  # item with that offset was never added

        try:
            if self.__items[index] is not False:
                raise KeyError(offset)  # item with that offset has already been removed
        except IndexError:
            raise KeyError(offset)  # item is out of range (too high)

        self.__items[index] = True

    def value(self) -> Offset:
        """
        Return the committable offset for this stream.
        """
        if not self.__index:
            return None

        try:
            index = self.__items.index(False)
        except ValueError:
            epoch = self.__index[-1]
            return epoch.offset + (len(self.__items) - epoch.index)

        epoch = self.__index[
            bisect.bisect_right([e.index for e in self.__index], index) - 1
        ]
        return epoch.offset + (index - epoch.index)
