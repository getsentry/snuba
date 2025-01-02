def fnv_1a(b: bytes) -> int:
    fnv_1a_32_prime = 16777619
    fnv_1a_32_offset_basis = 2166136261

    res = fnv_1a_32_offset_basis
    for byt in b:
        res = res ^ byt
        res = (res * fnv_1a_32_prime) & 0xFFFFFFFF  # force 32 bit
    return res


def fnv_1a_64(b: bytes) -> int:
    fnv_1a_64_prime = 1099511628211
    fnv_1a_64_offset_basis = 14695981039346656037

    res = fnv_1a_64_offset_basis
    for byt in b:
        res = res ^ byt
        res = (res * fnv_1a_64_prime) & 0xFFFFFFFFFFFFFFFF  # force 64 bit
    return res
