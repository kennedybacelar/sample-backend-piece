from typing import List
import pathspec
from pydantic.dataclasses import dataclass


@dataclass
class IgnoreSpec:
    patterns: List[str]

    @property
    def spec(self):
        if not hasattr(self, "_spec"):
            # pylint: disable=attribute-defined-outside-init
            self._spec = pathspec.PathSpec.from_lines(pathspec.patterns.GitWildMatchPattern, self.patterns)
        return self._spec

    def should_ignore(self, filename):
        return self.spec.match_file(filename)


default_ignorespec = IgnoreSpec(patterns=["vendor/"])
