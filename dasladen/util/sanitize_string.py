import petl as etl


def sanitize(v):
    """Convert ascii control chars ( < 32 ) into space"""
    if v:
        for x in [chr(c) for c in range(0, 32)]:
            v = v.replace(x, ' ')
    return v


# noinspection PyUnusedLocal
def transform(table, *fields, **args):
    if len(fields) > 0:
        return etl.convert(table, fields, sanitize)
    else:
        return etl.convertall(table, sanitize)
