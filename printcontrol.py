"""
this module is used for printing operations
"""

from sys import stdout


_last_line_length = 0
last_line = ""

def reprint(text):
    """
    prints a text on the same line
    call new_line() after all the calls of reprint to move to a new line and reset the internal variables
    """

    global  _last_line_length, last_line

    #clear line
    stdout.write('\r')
    stdout.write("".join([" "] * _last_line_length)) #print spaces to cover the last text printed before

    #return to start
    stdout.write('\r')

    #write updated line
    stdout.write(text)
    stdout.flush()

    #update last line length
    _last_line_length = len(text)
    last_line = text

def new_line():
    """
    moves to a new line
    """

    global _last_line_length, last_line

    stdout.write('\n')
    _last_line_length = 0
    last_line = ""
