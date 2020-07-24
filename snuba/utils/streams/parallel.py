from typing import MutableSequence, Optional


Offset = int


class OffsetTracker:
    def __init__(self, epoch: Offset) -> None:
        self.__epoch = epoch
        self.__completed: MutableSequence[Optional[bool]] = []

    def add(self, offset: Offset) -> None:
        """
        Add an offset to the set of in-progress items.
        """
        index = offset - self.__epoch
        if not index >= len(self.__completed):
            raise ValueError("offset must move monotonically")

        for i in range(len(self.__completed), index):
            self.__completed.append(None)

        self.__completed.append(False)

    def remove(self, offset: Offset) -> None:
        """
        Remove an offset from the set of in-progress items.
        """
        index = offset - self.__epoch
        if not index >= 0 or index > len(self.__completed):
            raise ValueError("offset out of range")

        if not self.__completed[index] is False:
            raise ValueError("offset is already untracked")

        self.__completed[index] = True

    def value(self) -> Offset:
        """
        Return the committable offset for this stream.
        """
        try:
            # Return the offset of the leftmost (earliest) incomplete item.
            return self.__epoch + self.__completed.index(False)
        except ValueError:
            # If all items are completed, the next incomplete item is going to
            # be the next offset we'd expect to add to the list.
            return self.__epoch + len(self.__completed)
