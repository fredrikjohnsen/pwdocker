#!/usr/bin/env python3

import os


def remove_file(file_path: str):
    if os.path.isfile(file_path):
        os.remove(file_path)
