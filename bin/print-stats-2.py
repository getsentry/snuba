# import profile
import pstats

# Read all 5 stats files into a single object
stats = pstats.Stats("bin/4.prof")
stats.strip_dirs()
stats.sort_stats("cumtime")

print("INCOMING CALLERS:")
stats.print_callers("\(find")

print("OUTGOING CALLEES:")
stats.print_callees("\(find")
