from typing import List
from dataclasses import dataclass


@dataclass
class SynthetizeOperation:
    variables: List[str]
