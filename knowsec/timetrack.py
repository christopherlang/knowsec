import datetime as dt


class Timetrack(object):
    """ Easily track time for logging and code performance
    Creating an object of this class will immediately begin tracking time since
    object construction. All date and time are stored as UTC time
    Multiple instances of time can be tracked by specifying the tag parameter
    that most methods have. The internal instance is 'root', but any name
    can be provided as well
    Some important methods would be:
      - now() : Returns the current UTC time. If time was paused, returns the
                the time when pausing occurred
      - elapsed() : Returns elapsed time since start, or when the object was
                    paused. The object returned are Python's timedelta objects
      - elapsed_seconds() : Similar to elapsed(), the difference being that the
                            number of seconds are returned instead of timedelta
      - elapsed_pretty() : Similar to elapsed(), but returns a string. The
                           string states the number of days, hours, minutes,
                           and seconds that has elapsed, depending on the how
                           long it actually has been
    """

    def __init__(self, initialize_time=True):
        self.int_time = dict()
        self.int_time['root'] = dict()
        self.int_time['root']['start'] = None
        self.int_time['root']['end'] = None

        if initialize_time is True:
            self.int_time['root']['start'] = self.now()

    def now(self, tag='root'):
        """Get the current date and time, in UTC, as Python datetime object
        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time
        Return:
          A datetime Python object, specifying the date and time in UTC
        """
        if self.int_time[tag]['end'] is None:
            return dt.datetime.utcnow()

        else:
            return self.int_time[tag]['end']

    def get_instances(self):
        """Get all instance names stored in this object
        Return:
          A list of strings, specifying instance names currently tracked
        """
        return list(self.int_time.keys())

    def reset_time(self, tag='root'):
        """Set the start time to current date and time
        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time
        Return:
          Nothing is returned
        """
        self.int_time[tag]['start'] = self.now(tag=tag)

    def new_time(self, tag):
        """Create new time instance named tag and start time
        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time
        Return:
          Nothing is returned
        """

        self.int_time[tag] = dict()
        self.int_time[tag]['start'] = dt.datetime.utcnow()
        self.int_time[tag]['end'] = None

    def pause_time(self, tag='root'):
        """Pause tracking of time
        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time
        Return:
          Nothing is returned
        """
        self.int_time[tag]['end'] = self.now(tag=tag)

    def unpause_time(self, tag='root'):
        """Set end time to None, effectively resetting pause time
        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time
        Return:
          Nothing is returned
        """
        self.int_time[tag]['end'] = None

    def set_time(self, date_time, tag='root'):
        """Sets start time to provided datetime object
        Please note that the datetime object provided MUST contain UTC time.
        Otherwise, all other returned times (e.g. elapsed) will be wrong
        Parameter:
          date_time : datetime.datetime
            A Python datetime object in UTC time
          tag : str
            Name of time instance. Default is 'root', the internal base time
        Return:
          Nothing is returned
        """
        self.int_time[tag]['start'] = date_time

    def get_start_time(self, tag='root'):
        """Get the stored start time
        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time
        Return:
          A datetime.datetime object, specifying the start time of instance
        """
        return self.int_time[tag]['start']

    def elapsed(self, tag='root'):
        """Get timedelta object, indicating elapse time since start
        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time
        Return:
          A datetime.timedelta object, specifying the instance's elapsed time
        """
        elapsed = self.now(tag=tag) - self.int_time[tag]['start']

        return elapsed

    def elapsed_seconds(self, tag='root'):
        """Get the number of seconds since start, as a float
        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time
        Return:
          A float, specifying the instance's elapsed time, in seconds
        """
        elapsed = self.elapsed(tag=tag)
        elapsed = elapsed.seconds + (elapsed.microseconds * 1e-6)

        return elapsed

    def elapsed_pretty(self, tag='root', **kwargs):
        """Get elapsed time as a string
        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time
        Return:
          A string, written in day/hour/minute/second, depending on length
        """
        elapsed = self.elapsed(tag=tag)
        elapsed_str = pretty_time_string(elapsed.days, elapsed.seconds,
                                         elapsed.microseconds, **kwargs)

        return elapsed_str

    def execution(self, tag='root'):
        """Get a dict with several time statistics
        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time
        Return:
          A dict, specifying the instance's time statistics
        """
        exec_time = execution_time(self.int_time[tag]['start'],
                                   self.now(tag=tag))

        return exec_time

    def start_time_pretty(self, tag='root'):
        """Get the start time, as a pretty formatted string
        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time
        Return:
          A string, specifying the instance's start time in pretty format
        """
        start_time = self.execution(tag=tag)['start']

        return start_time

    def end_time_pretty(self, tag='root'):
        """Get the end time, as a pretty formatted string
        Parameter:
          tag : str
            Name of time instance. Default is 'root', the internal base time
        Return:
          A string, specifying the instance's end time in pretty format
        """
        end_time = self.execution(tag=tag)['end']

        return end_time


def pretty_time_string(days=None, seconds=None, microseconds=None,
                       ndig_mins=2, ndig_secs=2):
    """Generate a pretty string, indicating time
    Generates a string, show elapsed time by:
      1. Number of days
      2. Number of hours
      3. Number of minutes
      4. Number of seconds
    Will only show if applicable (30 seconds has no days, or even minutes)
    """
    result = list()

    if days is not None:
        if days > 0:
            result.append(str(days) + " day(s)")

    total_seconds = seconds

    if microseconds is not None:
        total_seconds += (microseconds * 1e-6)

    if total_seconds >= 3600:
        total_hours = 0

        while total_seconds >= 3600:
            total_seconds -= 3600
            total_hours += 1

        result.append(str(total_hours) + " hour(s)")

    if total_seconds >= 60:
        total_minutes = 0

        while total_seconds >= 60:
            total_seconds -= 60
            total_minutes += 1

        total_minutes = round(total_minutes, ndig_mins)
        result.append(str(total_minutes) + " minute(s)")

    total_seconds = round(total_seconds, ndig_secs)
    result.append(str(total_seconds) + " second(s)")

    return ", ".join(result)


def execution_time(start_time, end_time):
    """Parse execution time in UTC
    Given two datetime objects, compute elapsed time in seconds and pretty
    string, generate formatted dates for both objects, and return original
    datetime objects
    Note: This function assumes that both start_time and end_time is UTC time.
          It will automatically apply a UTC time to the objects. If the objects
          are not UTC time then this application would have incorrect time
    Args:
        start_time, end_time (datetime):
            A datetime object
    Returns (dict):
    The dictionary has the following keys:
      1. pretty_str - a pretty string showing elapsed time
      2. seconds - Number of seconds elapsed between start_time and end_time
      3. start - formatted start date and time
      4. start_ios - ISO 8601 formatted start date and time
      5. end - formatted end date and time
      6. end_iso - ISO 8601 formatted start date and time
      7. raw - An array, containing start_time, end_time, and timedelta
    """
    result = dict()

    elapsed = end_time - start_time

    pretty_time = pretty_time_string(elapsed.days, elapsed.seconds,
                                     elapsed.microseconds)

    # if tz is None:
    #     tz = time.strftime("%z", time.gmtime())

    result['pretty_str'] = pretty_time

    result['start'] = start_time.strftime('%A %B %d %Y | %I:%M:%S%p UTC')
    result['start_iso'] = start_time.isoformat() + "Z"
    result['end'] = end_time.strftime('%A %B %d %Y | %I:%M:%S%p UTC')
    result['end_iso'] = end_time.isoformat() + "Z"

    result['seconds'] = elapsed.seconds + (elapsed.microseconds * 1e-6)

    result['raw'] = [start_time, end_time, elapsed]

    return result