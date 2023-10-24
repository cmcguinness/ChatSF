"""
    Print Loggging:

    Because the hosting service captures stdout and stderr and maintains logs directly,
    we just need to print() our messages to the logfile.  This is just to make it look
    decent and a bit easier to tweak.
"""
import os
import sys
import textwrap
from datetime import datetime
import time

class Plog:
    LEVEL_NONE = 0
    LEVEL_ERROR = 1
    LEVEL_WARN = 2
    LEVEL_INFO = 3
    LEVEL_DEBUG = 4
    LEVELS = ['None', 'Error', 'Warning', 'Info', 'Debug']

    _debug_level = LEVEL_DEBUG
    _print_time = True

    _print_file = None

    @classmethod
    def set_level(cls, newlevel):
        if isinstance(newlevel, str):
            for l, s in enumerate(cls.LEVELS):
                if s.lower() == newlevel.lower():
                    newlevel = l
                    break

        cls._debug_level = int(newlevel)

    @classmethod
    def usefile(cls, filename):
        cls._print_file = filename

    @classmethod
    def print_time(cls, pt):
        cls._print_time = pt

    @classmethod
    def print(cls, level, text):
        def expand(s, w):
            return s + ' ' * (w - len(s))

        if level > cls._debug_level:
            return

        if cls._print_file is not None:
            logfile = open(cls._print_file, "a")
        else:
            logfile = sys.stdout

        if len(text) == 0:
            # Just print out a blank line for a visual break in the log file
            print(f'', flush=True, file=logfile)
            return

        if cls._print_time:
            dt = ' (' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ')'
        else:
            dt = ''

        wid = max([len(s) for s in cls.LEVELS])

        if level <= cls._debug_level:
            if len(text) < 70:
                print(f'{expand(cls.LEVELS[level], wid)}{dt}: {text}', flush=True, file=logfile)
            else:
                first = f'{expand(cls.LEVELS[level], wid)}{dt}: '

                lines = text.split('\n')
                for line in lines:
                    single = textwrap.wrap(line)
                    for s in single:
                        print(f'{first}{s}', flush=True, file=logfile)
                        first = ' ' * len(first)

        if cls._print_file is not None:
            logfile.close()

    @classmethod
    def debug(cls, text: str):
        cls.print(cls.LEVEL_DEBUG, text)

    @classmethod
    def info(cls, text: str):
        cls.print(cls.LEVEL_INFO, text)

    @classmethod
    def warn(cls, text: str):
        cls.print(cls.LEVEL_WARN, text)

    @classmethod
    def error(cls, text: str):
        cls.print(cls.LEVEL_ERROR, text)

    @classmethod
    def log_to_file(cls):
        # If you want to delete all the old log files first
        for f in [fn for fn in os.listdir('.')]:
            if f.endswith('.log'):
                os.remove(f)
        logname = f'chatsf-{time.strftime("%Y%m%d-%H%M%S")}.log'
        Plog.usefile(logname)


#
#   Initialization code
#

# Use environment variables to set the logging level and file output
_log_level = os.getenv('LOG_LEVEL')
if _log_level is not None:
    Plog.set_level(_log_level)

_log_file = os.getenv('LOG_FILE')
if _log_file is not None:
    Plog.log_to_file()
