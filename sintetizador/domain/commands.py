from typing import List
from dataclasses import dataclass


@dataclass
class SynthetizeSystem:
    variables: List[str]


@dataclass
class SynthetizeExecution:
    variables: List[str]


@dataclass
class SynthetizeOperation:
    variables: List[str]
