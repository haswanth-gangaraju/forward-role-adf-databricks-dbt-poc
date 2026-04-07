"""
conftest.py — adds the project root to sys.path so tests can import modules.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
