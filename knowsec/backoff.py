import time
import random


def exponential_backoff(max_wait, verbose=True):
    """Backoff a call exponentially

    Returns a function that when called, will execute `time.sleep` to stop
    the line of execution

    Time spent sleeping follows an exponential backoff algorithm, as dictated
    by Google. The time spent waiting follows the formula:
        wait time = min(2 ** n + random milliseconds, maximum backoff)
            Where 'n' is the number of retries
            Where 'random milliseconds' is a value less than or equal to 1000
            Where 'maximum backoff' is the user set maximum backoff time

    >>> bckoff = exponential_backoff(32)  # max wait time set to 32 seconds
    >>> bckoff()
    Sleeping for 1.43 seconds
    {'ntries': 1, 'wait_time': 1.426}
    >>> bckoff()
    Sleeping for 2.75 seconds
    {'ntries': 2, 'wait_time': 2.75}
    >>> bckoff(False)
    {'ntries': 0, 'wait_time': 0}
    >>> bckoff()
    Sleeping for 1.01 seconds
    {'ntries': 1, 'wait_time': 1.01}

    Parameters
    ----------
    max_wait : int or float
        The maximum time to wait, in seconds
    verbose : bool
        If waiting, should it print how many seconds it is doing so

    Returns
    -------
    function
        A function that can be executed to wait for a certain amount of time
        It has one parameter `should_wait`. If `True`, then the function will
        sleep at a determined time. If `False`, then it will reset the internal
        count
    """
    backoff_ntries = 0

    def backoff_fun(should_wait=True):
        """Exponential Backoff closure

        Parameters
        ----------
        should_wait : bool
            If `True`, the function will sleep, following an exponential
            backoff scheme, up to the set `max_wait`. If `False` then the
            internal count of retries is reset to zero

        Returns
        -------
        dict
            A dictionary with two keys: 'ntries', specifying how many the
            number of retries, and 'wait_time', specifying how many seconds
            it had just slept on
        """
        nonlocal backoff_ntries

        if should_wait is False:
            backoff_ntries = 0
            wait_time = 0
        else:
            wait_time = (2 ** backoff_ntries)
            wait_time += float(random.randint(0, 1000) / 1000)
            wait_time = min(wait_time, max_wait)

            backoff_ntries += 1

            if verbose is True:
                print(f'Sleeping for {wait_time:.2f} seconds')

            time.sleep(wait_time)

        return {'ntries': backoff_ntries, 'wait_time': wait_time}

    return backoff_fun


def jittered_backoff(max_wait, base_wait=1, verbose=True):
    """Backoff a call exponentially with decorrelated jitter

    Time spent sleeping follows an exponential backoff algorithm with jitter,
    as dictated by AWS

    >>> bckoff = jittered_backoff(32)  # max wait time set to 32 seconds
    >>> bckoff()
    Sleeping for 1.01 seconds
    {'ntries': 1, 'wait_time': 1.011439601853178}
    >>> bckoff()
    Sleeping for 2.34 seconds
    {'ntries': 2, 'wait_time': 2.336598367699084}
    >>> bckoff(False)
    {'ntries': 0, 'wait_time': 0}
    >>> bckoff()
    Sleeping for 1.20 seconds
    {'ntries': 1, 'wait_time': 1.203851331708007}

    Parameters
    ----------
    max_wait : int or float
        The maximum time to wait, in seconds
    base_wait : int or float
        The minimum time to wait, in seconds
    verbose : bool
        If waiting, should it print how many seconds it is doing so

    Returns
    -------
    function
        A function that can be executed to wait for a certain amount of time
        It has one parameter `should_wait`. If `True`, then the function will
        sleep at a determined time. If `False`, then it will reset the internal
        count
    """

    backoff_ntries = 0

    def backoff_fun(should_wait=True):
        """Exponential Backoff closure

        Parameters
        ----------
        should_wait : bool
            If `True`, the function will sleep, following an exponential
            backoff scheme, up to the set `max_wait`. If `False` then the
            internal count of retries is reset to zero

        Returns
        -------
        dict
            A dictionary with two keys: 'ntries', specifying how many the
            number of retries, and 'wait_time', specifying how many seconds
            it had just slept on
        """
        nonlocal backoff_ntries

        if should_wait is False:
            backoff_ntries = 0
            wait_time = 0
        else:
            wait_time = min(max_wait, base_wait * 2 ** backoff_ntries)
            wait_time = wait_time / 2 + random.uniform(0, wait_time / 2)
            wait_time = min(max_wait, random.uniform(base_wait, wait_time * 3))

            backoff_ntries += 1

            if verbose is True:
                print(f'Sleeping for {wait_time:.2f} seconds')

            time.sleep(wait_time)

        return {'ntries': backoff_ntries, 'wait_time': wait_time}

    return backoff_fun


def linear_backoff(max_wait, initial_wait=1, step=1, verbose=True):
    """Backoff a call linearly

    Returns a function that when called, will execute `time.sleep` to stop
    the line of execution

    Time spent sleeping follows a constant, step-wise increase:
        wait time = min(initial_wait + (n * step), maximum backoff)
            Where 'n' is the number of retries
            Where 'step' is the linear increase of time, in seconds
            Where 'maximum backoff' is the user set maximum backoff time

    >>> bckoff = linear_backoff(32)  # max wait time set to 32 seconds
    >>> bckoff()
    Sleeping for 1.00 seconds
    {'ntries': 1, 'wait_time': 1}
    >>> bckoff()
    Sleeping for 2.00 seconds
    {'ntries': 2, 'wait_time': 2}
    >>> bckoff = linear_backoff(32, step=3)  # Increment wait time by 3 seconds
    >>> bckoff()
    Sleeping for 1.00 seconds
    {'ntries': 1, 'wait_time': 1}
    >>> bckoff()
    Sleeping for 4.00 seconds
    {'ntries': 2, 'wait_time': 4}

    Parameters
    ----------
    max_wait : int or float
        The maximum time to wait, in seconds
    initial_wait : int or float
        The starting wait time in seconds
    step : int or float
        The increase of the waiting time per retries
    verbose : bool
        If waiting, should it print how many seconds it is doing so

    Returns
    -------
    function
        A function that can be executed to wait for a certain amount of time
        It has one parameter `should_wait`. If `True`, then the function will
        sleep at a determined time. If `False`, then it will reset the internal
        count
    """
    backoff_ntries = 0

    def backoff_fun(should_wait=True):
        """Linear Backoff closure

        Parameters
        ----------
        should_wait : bool
            If `True`, the function will sleep, following an linear
            backoff scheme, up to the set `max_wait`. If `False` then the
            internal count of retries is reset to zero

        Returns
        -------
        dict
            A dictionary with two keys: 'ntries', specifying how many the
            number of retries, and 'wait_time', specifying how many seconds
            it had just slept on
        """
        nonlocal backoff_ntries
        nonlocal max_wait

        if should_wait is False:
            backoff_ntries = 0
            wait_time = 0
        else:
            wait_time = min(initial_wait + (backoff_ntries * step),
                            max_wait)

            backoff_ntries += 1

            if verbose is True:
                print(f'Sleeping for {wait_time:.2f} seconds')

            time.sleep(wait_time)

        return {'ntries': backoff_ntries, 'wait_time': wait_time}

    return backoff_fun


def constant_backoff(backoff, verbose=True):
    """Backoff a call with the same wait time

    Returns a function that when called, will execute `time.sleep` to stop
    the line of execution
    The time spent sleeping is always the same

    >>> bckoff = constant_backoff(2)  # Wait for 2 seconds per call
    >>> bckoff()
    Sleeping for 2.00 seconds
    {'ntries': 1, 'wait_time': 2}
    >>> bckoff()
    Sleeping for 2.00 seconds
    {'ntries': 2, 'wait_time': 2}

    Parameters
    ----------
    backoff : int or float
        The wait time, in seconds
    verbose : bool
        If waiting, should it print how many seconds it is doing so

    Returns
    -------
    function
        A function that can be executed to wait for a certain amount of time
        It has one parameter `should_wait`. If `True`, then the function will
        sleep at a determined time. If `False`, then it will reset the internal
        count
    """
    backoff_ntries = 0

    def backoff_fun(should_wait=True):
        """Constant Backoff closure

        Parameters
        ----------
        should_wait : bool
            If `True`, the function will sleep, following an linear
            backoff scheme, up to the set `max_wait`. If `False` then the
            internal count of retries is reset to zero

        Returns
        -------
        dict
            A dictionary with two keys: 'ntries', specifying how many the
            number of retries, and 'wait_time', specifying how many seconds
            it had just slept on
        """
        nonlocal backoff
        nonlocal backoff_ntries

        if should_wait is False:
            backoff_ntries = 0
            wait_time = 0
        else:
            wait_time = backoff

            backoff_ntries += 1

            if verbose is True:
                print(f'Sleeping for {wait_time:.2f} seconds')

            time.sleep(wait_time)

        return {'ntries': backoff_ntries, 'wait_time': wait_time}

    return backoff_fun


if __name__ == "__main__":
    import doctest
    random.seed(12345)

    doctest.testmod()
