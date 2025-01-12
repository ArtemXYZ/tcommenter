"""
    Модуль тестирования проекта.
"""

import unittest
import pytest

from your_module.core import some_function

def test_some_function():
    assert some_function(2) == 4
    assert some_function(0) == 0