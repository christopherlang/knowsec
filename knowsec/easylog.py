import logging
import os
import datetime as dt


class Easylog:
    """Fast and easy logging with Easylog

    The `Easylog` is a class designed to make logging a lot easier and faster
    by targeting the most common types of logging (console, file) and setting
    them up with usable default settings. `Easylog` is essentially a simplified
    wrapper around Python's `logging` module

    Easylog has built-in support for console and file logging, and supports any
    number of those. This is especially useful for file logging, where you can
    create a number of different log files for different types of log messages
    e.g. one log for information, one for errors, and another for debugging

    Console logging is specifically set up for replacing `print` statements,
    and similar to file logging, can have multiple loggers for varying purposes

    Example:
        Easylog, by default, creates a console logger named `console0`, and
        sets the default global logging level to 'info'::

            >>> import easylog
            >>> mylogger = easylog.Easylog()
            >>> mylogger.log_info("This is a test")
            INFO - This is a test

        Multiple console and file loggers are supported. For example, say two
        different console loggers with different log levels

            >>> mylogger = easylog.Easylog(create_console=False)
            >>> mylogger.add_consolelogger(loglevel='error')
            >>> mylogger.add_consolelogger(loglevel='critical')
            >>> mylogger.log_critical("This is a critical message")
            CRITICAL - This is a critical message
            CRITICAL - This is a critical message
            >>> mylogger.log_error('This is an error message')
            ERROR - This is an error message

        FYI, there are two critical messages above because one of the console
        loggers has a log level of `error`, which is a lower severity than
        `critical`

    Log levels:
        Log levels in `Easylog` are the same ones defined in `logging`. They're
        integers that define the severity of the log message. A logger's log
        level determines which message they'll receive and process

        Check the README for a detailed description if you do not understand
        how Python's `logging` log levels work

    Default Settings:
        Easylog has several built-in default settings to get logging up and
        running as quickly as possible:
            - console logging has the following defaults:
                * Streamed (or prints) to your standard out
                * Format '%(levelname)s - %(message)s'. All it prints is the
                  logging level and message
                * Logging level set to your global logging level
            - file logging has the following defaults:
                * Semi-automated file naming with your preferred prefix,
                  date and time, and file extension '.log'
                * Format '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

        It is possible to change these settings, such as log levels and
        formats easily with the appropriate methods

    Attributes:
        globallevel : int
            This is the global logging level set during object construction
    """

    def __init__(self, loggername=None, globallevel='info',
                 create_console=True):
        """ Easylog constructor

        Creates an instance of `Easylog`

        Arguements:
            loggername : str
                The logger's name, used to different the instance under the
                `logging` hierarchy. If set to `None`, will use the module's
                name (usually `easylog`)
            globallevel : str (defaults to 'info')
                The global logging level. When using the add_*logger methods,
                you're given the option to change the logger's level. If it
                is not set, the logger's log level is set to this

                Permitted values: 'critical','error','warning','info','debug'
            create_console : bool (defaults to True)
                If true, `Easylog` will create a console logger with default
                settings
        """
        self._handlers = list()
        self._loggername = __name__ if loggername is None else loggername
        self._globallevel = _string2loglevel(globallevel)
        self._filecounter = 0
        self._namecounters = {'file': 0, 'console': 0, 'stream': 0}
        self._dateformats = {'file': "%Y-%m-%dT%H:%M:%S",
                             'stream': "%Y-%m-%dT%H:%M:%S",
                             'console': "%I:%M:%S %p"}
        self._lognames = list()

        # logging.basicConfig(level=logging.DEBUG)
        self._logger = logging.getLogger(self._loggername)
        self._logger.setLevel(logging.DEBUG)

        if create_console is True:
            self.add_consolelogger()

        self._handlernames = self._get_handler_names()

    @property
    def globallevel(self):
        """The global logging level

        str: When using the add_*logger methods, you're given the option to
            change the logger's level. If it is not set, the logger's log level
            is set to this
        """
        return self._globallevel

    @property
    def handlernames(self):
        return self._handlernames

    @handlernames.getter
    def handlernames(self):
        """Names assigned to logging handlers

        list of str: A list of the names of handlers
        """
        return self._get_handler_names()

    def _log_controls(self, logtype, logname=None, loglevel=None,
                      logformat=None, dateformat=None):
        if logname is None:
            logname = logtype + str(self._namecounters[logtype])
            self._namecounters[logtype] += 1
        else:
            if logname in self._get_handler_names():
                raise ValueError("Log name {0} already in use".format(logname))

        if loglevel is None:
            loglevel = self._globallevel
        else:
            loglevel = _string2loglevel(loglevel)

        if logformat is None:
            logformat = _default_log_format(logtype)

        if dateformat is None:
            dateformat = self._dateformats[logtype]

        logformat = logging.Formatter(logformat, dateformat)

        log_controls = {'logtype': logtype, 'logname': logname,
                        'loglevel': loglevel, 'logformat': logformat,
                        'dateformat': dateformat}

        return log_controls

    def _add_logger(self, log_handler, log_controls):
        log_handler.setLevel(log_controls['loglevel'])
        log_handler.setFormatter(log_controls['logformat'])

        self._logger.addHandler(log_handler)

        log_rec = _logger_record(log_handler, log_controls['logname'],
                                 log_controls['logtype'],
                                 log_controls['loglevel'],
                                 log_controls['dateformat'])

        self._handlers.append(log_rec)

    def add_streamlogger(self, stream, logname=None, loglevel=None,
                         logformat=None, dateformat=None):
        log_controls = self._log_controls('stream', logname, loglevel,
                                          logformat, dateformat)
        log_handler = logging.StreamHandler(stream=stream)

        self._add_logger(log_handler, log_controls)

    def add_consolelogger(self, logname=None, loglevel=None, logformat=None,
                          dateformat=None):
        """ Add a console logger

        Prints log statements to console. Creates a `logging.StreamHandler`
        internally to handle console logging

        Arguements:
            logname : str (default `None`)
                The name of the handler. If `None`, a name is automatically
                assigned as `console`, plus a counter e.g. `console0`
            loglevel : str (default `None`)
                The log level of the handler. Lowercase names of `logging` log
                levels i.e. 'info', 'critical', etc. If `None, it is set to
                the global log level. See `Easylog.globallevel`
            logformat : str (default `None`)
                The log format for the handler. The same format as defined in
                Python's `logging` module. If `None`, sets internal defaults
            dateformat : str (default `None`)
                The date format for the handler. The same as used in the
                `datetime` module. If `None`, sets internal defaults
            stream
                The values to be passed to `stream` is the same as
                `logging.StreamHandler`. If `None`, than log statements will be
                sent to `sys.stderr`, often the console
        """
        log_controls = self._log_controls('console', logname, loglevel,
                                          logformat, dateformat)
        log_handler = logging.StreamHandler(stream=None)

        self._add_logger(log_handler, log_controls)

    def add_filelogger(self, logpath, appendtime=True, logname=None,
                       loglevel=None, logformat=None, dateformat=None,
                       encoding='utf-8', mode='a', delay=False):
        """ Add a file logger

        Create a log file `logpath`. Path names are considered. If only a
        name is provided, will create the log file in current working directory

        Creates a `logging.FileHandler` to handle the file logging. Some of the
        parameters are simply passed to that class

        Arguements:
            logpath : str
                Filename of the log file. Paths can be included. If only a name
                is provided, will create the log file in current working
                directory
            appendtime : bool (default `True`)
                Should the log's filename have date and time appended. If
                `True` Then a datetime UTC string is appended with a hyphen
                i.e. %Y%m%dT%H%M%SZ
            logname : str (default `None`)
                The name of the handler. If `None`, a name is automatically
                assigned as `file`, plus a counter e.g. `file0`
            loglevel : str (default `None`)
                The log level of the handler. Lowercase names of `logging` log
                levels i.e. 'info', 'critical', etc. If `None, it is set to
                the global log level. See `Easylog.globallevel`
            logformat : str (default `None`)
                The log format for the handler. The same format as defined in
                Python's `logging` module. If `None`, sets internal defaults
            dateformat : str (default `None`)
                The date format for the handler. The same as used in the
                `datetime` module. If `None`, sets internal defaults
            encoding : str (default 'utf-8')
                The encoding of the file. Try to stick with `utf-8`. This
                parameter is the same as `logging.FileHandler`
            mode : str (default 'a')
                If set to `a`, file logging is in append mode (recommended).
                This parameter is the same as `logging.FileHandler`
            delay : bool (default `False`)
                Delay the opening and writing of the file. The default `False`
                is recommended for most cases. This parameter is the same as
                `logging.FileHandler`
        """
        if appendtime is True:
            logpath = _append_time(logpath)

        log_controls = self._log_controls('file', logname, loglevel,
                                          logformat, dateformat)
        log_handler = logging.FileHandler(logpath, mode, encoding, delay)

        self._add_logger(log_handler, log_controls)

    def set_logformat(self, handlername, fmt, dateformat=None):
        """Change a handler's log format

        Change the logging format for a handler's output statements

        Arguements:
            handlername : str
                The name of the handler. See the property `handlernames` to
                find out the names of created handlers
            fmt : str
                The new log format for the handler. The same format as defined
                in Python's `logging` module
            dateformat : str (default `None`)
                The new date format for the handler. The same as used in the
                `datetime` module. If `None` than the date format is not
                changed
        """
        if self._handlers:
            errmsg = "No Logging Handlers have been defined"
            raise NoDefinedHandlersError(errmsg)
        else:
            handler_records = [a_handler for a_handler in self._handlers
                               if a_handler['name'] == handlername]

            if handler_records:
                errmsg = "No handler of the name '{0}' was found"
                errmsg = errmsg.format(handlername)

                raise NoHandlersFoundError(errmsg)
            else:
                handler_rec = handler_records[0]

                if dateformat is None:
                    dateformat = handler_rec['dateformat']

                fmt = logging.Formatter(fmt, dateformat)
                handler_records['handler'].setFormatter(fmt)

    def _get_handler_names(self):
        result = [a_logger['name'] for a_logger in self._handlers]

        return result

    def log_critical(self, msg):
        """Log a message as critical

        Critical is the most severe log message, often used when an error is
        encountered that terminates the program/software

        Handlers set to 'critical' log level will log the following messages:
        'critical' only

        Arguments:
            msg : str
                The message to be logged
        """
        self._logger.critical(msg)

    def log_error(self, msg):
        """Log a message as error

        Error is the second most severe log message, often used when an
        error is encountered but does not terminate the program/software.
        However, the results (e.g. data processing) is likely incorrect and is
        recommended that the developer check the log

        Handlers set to 'error' log level will log the following messages:
        'error', 'critical'

        Arguments:
            msg : str
                The message to be logged
        """
        self._logger.error(msg)

    def log_warning(self, msg):
        """Log a message as warning

        Warning is the third most severe log message, often used when a warning
        is encountered (e.g. module developed for version 3.x.x when you're
        using 2.x). This would not terminate the program, but the results
        (e.g. data processing) might be incorrect

        Handlers set to 'warning' log level will log the following messages:
        'warning', 'error', 'critical'

        Arguments:
            msg : str
                The message to be logged
        """
        self._logger.warning(msg)

    def log_info(self, msg):
        """Log a message as info

        Info is the basic type of log messages e.g. logging useful information
        at runtime, such as count of something. This is a 'safe' message, and
        does not indicate that the program has encountered a problem

        Handlers set to 'info' log level will log the following messages:
        'info', 'warning', 'error', 'critical'

        Arguments:
            msg : str
                The message to be logged
        """
        self._logger.info(msg)

    def log_debug(self, msg):
        """Log a message as debug

        Debug is typically used when the program is set in some kind of
        'debug mode'. Messages logged at the 'debug' level would generally
        be highly detailed, exposing the inner working of the program for
        debugging purposes

        Handlers set to 'debug' log level will log the following messages:
        'debug', 'info', 'warning', 'error', 'critical'

        Arguments:
            msg : str
                The message to be logged
        """
        self._logger.debug(msg)


def _default_log_format(handlertype):
    handler_format = None

    if handlertype == 'console':
        handler_format = '%(levelname)s - %(message)s'
    elif handlertype == 'file':
        handler_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    elif handlertype == 'stream':
        handler_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    elif handlertype == 'module':
        handler_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    return handler_format


def _string2loglevel(loglevel):
    logging_object_level = None

    loglevel = loglevel.lower()

    if loglevel == "critical":
        logging_object_level = logging.CRITICAL
    elif loglevel == "error":
        logging_object_level = logging.ERROR
    elif loglevel == "warning":
        logging_object_level = logging.WARNING
    elif loglevel == "info":
        logging_object_level = logging.INFO
    elif loglevel == "debug":
        logging_object_level = logging.DEBUG
    else:
        levels = "'critical', 'error', 'warning', 'info', 'debug'"
        msg = "'loglevel' must be one of: " + levels
        raise ValueError(msg)

    return logging_object_level


def _logger_record(handler, name, loggertype, loglevel, dateformat):
    record = {'handler': handler, 'name': name, 'loggertype': loggertype,
              'loglevel': loglevel, 'dateformat': dateformat}

    return record


def _filename_splitter(thepath):
    result = list()
    result.append(os.path.dirname(thepath))
    result.append(os.path.splitext(os.path.basename(thepath))[0])
    result.append(os.path.splitext(os.path.basename(thepath))[1])

    return tuple(result)


def _append_time(thepath):
    path_split = _filename_splitter(thepath)
    str_time = dt.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    filename = path_split[1]
    filename += "-" + str_time
    filename += path_split[2]

    newpath = os.path.join(path_split[0], filename)

    return newpath


class Error(Exception):
    """Base class for exceptions in Easylog"""
    pass


class NoDefinedHandlersError(Error):
    """Exception raised there are no Logging Handlers defined in Easylog

    Raised only when there is attempt to modify a handler

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = "No Logging Handlers have been defined"


class NoHandlersFoundError(Error):
    """Exception raised there are no Logging Handlers found

    Raised only when there is attempt to modify a handler

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
