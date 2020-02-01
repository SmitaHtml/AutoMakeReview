# ANSI escape codes

# Text color change
black_text = "\x1b[1;30m"
red_text = "\x1b[1;31m"
green_text = "\x1b[1;92m"
yellow_text = "\x1b[1;33m"
blue_text = "\x1b[1;34m"
purple_text = "\x1b[1;35m"
cyan_text = "\x1b[9;36m"
white_text = "\x1b[1;37m"

# Background color change
black_background = "\x1b[1;40m"
red_background = "\x1b[1;41m"
green_background = "\x1b[1;42m"
yellow_background = "\x1b[1;43m"
blue_background = "\x1b[1;44m"
purple_background = "\x1b[1;45m"
cyan_background = "\x1b[1;46m"
white_background = "\x1b[1;47m"

cross_through_text = "\x1b[9m"
under_lined_text = "\x1b[4m"
revers_text_and_background = "\x1b[7m"

end_text_change = "\x1b[0m"

# clear_screen = "\x1b[2J" # puts it at bottom of terminal window
clear_screen = "\x1b[2J\x1b[H"

