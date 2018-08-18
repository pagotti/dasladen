import petl as etl


def empty_as_none(v):
    """Convert empty string into null"""
    if (not v) and (v == ''):
        return None
    else:
        return v


# noinspection PyUnusedLocal
def transform(table, *fields, **args):
    if len(fields) > 0:
        return etl.convert(table, fields, empty_as_none)
    else:
        return etl.convertall(table, empty_as_none)


