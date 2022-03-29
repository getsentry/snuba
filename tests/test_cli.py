import signal
import subprocess
import time


class TestCli(object):
    def test_consumer_cli(self) -> None:
        """
        Check that the consumer daemon runs until it is killed
        """
        proc = subprocess.Popen(["snuba", "consumer", "--storage", "errors"])
        time.sleep(3)
        proc.poll()
        assert proc.returncode is None  # still running

        proc.send_signal(signal.SIGINT)
        proc.wait()
