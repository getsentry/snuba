import fcntl
import os
import signal
import subprocess
import time

def non_block_read(output):
    fd = output.fileno()
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
    try:
        return output.read()
    except:
        return ''


class TestCli(object):

    def test_consumer_cli(self):
        """
        Check that the consumer daemon runs until it is killed
        """
        proc = subprocess.Popen(['snuba', 'consumer'], stderr=subprocess.PIPE)
        time.sleep(1)
        proc.poll()

        # check it's still running with no stderr output
        assert non_block_read(proc.stderr) == ''
        assert proc.returncode is None

        # kill it
        proc.send_signal(signal.SIGINT)
        proc.wait()
