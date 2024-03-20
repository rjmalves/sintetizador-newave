import re
from typing import List


def match_variables_with_wildcards(
    given_variables: List[str], all_variables: List[str]
):
    variables_with_wildcards: List[str] = []
    for v in given_variables:
        if "*" in v:
            variables_with_wildcards += [
                matched_v
                for matched_v in all_variables
                if re.search(v.replace("*", ".*"), matched_v)
            ]
        else:
            variables_with_wildcards.append(v)
    return variables_with_wildcards
