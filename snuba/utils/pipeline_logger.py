from __future__ import annotations

from typing import Any, Iterator


class PipelineLogger:
    """
    Used for tracking intermediate IO contracts of pipelines in order to break them apart.

    Example:
    say you have tests for the following
    def do_something_big(input):
        s1 = do_small1(input)
        s2 = do_small2(s1)
        output = do_small3(s2)
        return output
    if we wanted to break it up, we could capture intermediate state with the logger:
    import PipelineLogger
    plog = PipelineLogger()
    def do_something_big(input):
        plog.begin(input)
        s1 = do_small1(input)
        plog.pipe(s1)
        s2 = do_small2(s1)
        plog.pipe(s2)
        output = do_small3(s2)
        plog.log(output)
        return output
    # plog now looks something like [(input, s1), (s1, s2), (s2, out)]
    """

    def __init__(self) -> None:
        # the inner list is a "trace" (not sentry trace): -> t1 -> t2 -> t3 ->
        # the tuple is (in, out)
        self.logged: list[list[tuple[Any, Any]]] = []

    def begin(self, val: Any) -> None:
        """
        Begins a new "trace" (not sentry trace): -> t1 -> t2 -> t3 ->
        """
        self.logged.append([])
        self.isInput = True
        self.log(val)

    def log(self, val: Any) -> None:
        """
        Logs the given val as the current output/input.
        Swaps between treating val as input or output.
        eg. log(1) log(2) log(3) log(4)
            [(1,2),(3,4)]
        """
        if len(self.logged) == 0:
            raise ValueError("Begin must be called before eat")

        if self.isInput:
            self.logged[len(self.logged) - 1].append((val, None))
        else:
            # (val, None) -> (val, newval)
            t = self.logged[len(self.logged) - 1]
            t[len(t) - 1] = (t[len(t) - 1][0], val)

        self.isInput = not self.isInput

    def pipe(self, val: Any) -> None:
        """
        Logs the given val as the current output and the next input
        """
        self.log(val)
        self.log(val)

    def __getitem__(self, val: Any) -> list[tuple[Any, Any]]:
        return self.logged[val]

    def __iter__(self) -> Iterator[list[tuple[Any, Any]]]:
        return iter(self.logged)
