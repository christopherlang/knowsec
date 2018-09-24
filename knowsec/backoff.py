import time
import random


def exponential_backoff(max_backoff, verbose=True):
    """Backoff a call exponentially

    Returns a function that when called, will execute `time.sleep` to stop
    the line of execution

    Time spent sleeping follows an exponential backoff algorithm, as dictated
    by Google. The time spent waiting follows the formula:

        wait time = min(2 ** n + random milliseconds, maximum backoff)
            Where 'n' is the number of retries
            Where 'random milliseconds' is a value less than or equal to 1000
            Where 'maximum backoff' is the user set maximum backoff time

    Parameters
    ----------
    max_backoff : int or float
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
            backoff scheme, up to the set `max_backoff`. If `False` then the
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
            wait_time = min(wait_time, max_backoff)

            backoff_ntries += 1

            if verbose is True:
                print(f'Sleeping for {wait_time:.2f} seconds')

            time.sleep(wait_time)

        return {'ntries': backoff_ntries, 'wait_time': wait_time}

    return backoff_fun


def linear_backoff(max_backoff, initial_wait=1, step=1, verbose=True):
    """Backoff a call exponentially

    Returns a function that when called, will execute `time.sleep` to stop
    the line of execution

    Time spent sleeping follows a constant, step-wise increase:

    wait time = min(initial_wait + (n * step), maximum backoff)
        Where 'n' is the number of retries
        Where 'step' is the linear increase of time, in seconds
        Where 'maximum backoff' is the user set maximum backoff time

    Parameters
    ----------
    max_backoff : int or float
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
            backoff scheme, up to the set `max_backoff`. If `False` then the
            internal count of retries is reset to zero

        Returns
        -------
        dict
            A dictionary with two keys: 'ntries', specifying how many the
            number of retries, and 'wait_time', specifying how many seconds
            it had just slept on
        """
        nonlocal backoff_ntries
        nonlocal max_backoff

        if should_wait is False:
            backoff_ntries = 0
            wait_time = 0
        else:
            wait_time = min(initial_wait + (backoff_ntries * step),
                            max_backoff)

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
            backoff scheme, up to the set `max_backoff`. If `False` then the
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
