# import profile
import pstats

# from profile_fibonacci_memoized import fib, fib_seq

# Read all 5 stats files into a single object
stats = pstats.Stats("bin/4.prof")
stats.strip_dirs()
stats.sort_stats("tottime")

# limit output to lines with "(fib" in them
stats.print_stats()
