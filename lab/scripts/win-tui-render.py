#!/usr/bin/env python3
"""Replay a raw terminal stream into a screen buffer and print the final frame.

Handles the subset of ANSI that ratatui emits: CUP, ED, EL, SGR (ignored),
plus the alternate-screen switches.
"""
import re, sys

ROWS, COLS = 50, 200
data = open(sys.argv[1], "rb").read().decode("utf8", "replace")

# A mark names a moment; render everything drawn up to it.
if len(sys.argv) > 2:
    idx = data.find("<<<MARK:" + sys.argv[2] + ">>>")
    if idx >= 0:
        data = data[:idx]

screen = [[" "] * COLS for _ in range(ROWS)]
row = col = 0
i = 0
csi = re.compile(r"\x1b\[([0-9;?]*)([@-~])")
while i < len(data):
    ch = data[i]
    if ch == "\x1b":
        m = csi.match(data, i)
        if m:
            params, final = m.group(1), m.group(2)
            nums = [int(p) for p in params.split(";") if p.isdigit()]
            if final == "H":
                row = (nums[0] - 1) if nums else 0
                col = (nums[1] - 1) if len(nums) > 1 else 0
            elif final == "J":
                mode = nums[0] if nums else 0
                if mode == 2:
                    screen = [[" "] * COLS for _ in range(ROWS)]
                    row = col = 0
            elif final == "K":
                for c in range(col, COLS):
                    screen[row][c] = " "
            elif final == "A":
                row = max(0, row - (nums[0] if nums else 1))
            elif final == "B":
                row = min(ROWS - 1, row + (nums[0] if nums else 1))
            elif final == "C":
                col = min(COLS - 1, col + (nums[0] if nums else 1))
            elif final == "D":
                col = max(0, col - (nums[0] if nums else 1))
            i = m.end()
            continue
        if data.startswith("\x1b]", i):
            end = data.find("\x07", i)
            i = (end + 1) if end >= 0 else i + 2
            continue
        i += 2
        continue
    if ch == "\r":
        col = 0
    elif ch == "\n":
        row = min(ROWS - 1, row + 1)
    elif ch == "\b":
        col = max(0, col - 1)
    elif ch >= " ":
        if row < ROWS and col < COLS:
            screen[row][col] = ch
        col += 1
        if col >= COLS:
            col = 0
            row = min(ROWS - 1, row + 1)
    i += 1

for line in screen:
    print("".join(line).rstrip())
