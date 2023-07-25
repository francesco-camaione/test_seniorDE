from typing import List


def remove_matching_values(input_list: List[str], values_to_remove: List[str]) -> List[str]:
    lst = [x for x in input_list if x not in values_to_remove]
    return lst
