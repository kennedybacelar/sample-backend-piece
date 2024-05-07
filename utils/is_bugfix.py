from typing import List


def calculate_is_bugfix(labels: List[str], title: str) -> bool:
    """
    :rtype: bool
    :param labels: labels if pull request
    :param title: title of pull request
    :return: true if either title contains 'bug' or 'fix' in any case or if any label contains 'bug' or 'fix' in any case
    """

    def __is_bugfix__(row: str) -> bool:
        return "fix" in row.lower() or "bug" in row.lower()

    return __is_bugfix__(title) or (len(list(filter(__is_bugfix__, labels))) > 0)
