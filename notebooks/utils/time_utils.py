from datetime import datetime, timedelta

def parse_time(ts):
    """
    Parses a timestamp value and returns a corresponding `datetime` object.

    This function handles two common timestamp formats:
    - ISO 8601 strings (e.g., `"2025-01-01T12:30:00"`), which are parsed using `datetime.fromisoformat`.
    - Unix timestamps (e.g., `1672531200.0`), which are parsed using `datetime.fromtimestamp`.

    The function automatically detects the input type and applies the appropriate parsing logic.

    Parameters
    ----------
    ts : str or float or int
        The timestamp to parse. Can be either:
        - A string in ISO 8601 format (e.g., `"2025-05-11T08:00:00"`), or
        - A Unix timestamp as a float or int (e.g., `1715419200`).

    Returns
    -------
    datetime.datetime
        A `datetime` object representing the parsed timestamp.

    Raises
    ------
    ValueError
        If the input is a string but not in valid ISO 8601 format.
    TypeError
        If the input is not a string, float, or int.

    Examples
    --------
    >>> parse_time("2025-05-11T08:00:00")
    datetime.datetime(2025, 5, 11, 8, 0)

    >>> parse_time(1715419200)
    datetime.datetime(2024, 5, 11, 8, 0)
    """
    if isinstance(ts, str):
        return datetime.fromisoformat(ts)
    return datetime.fromtimestamp(ts)
