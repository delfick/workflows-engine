import attrs

from . import application


@attrs.define
class Computation:
    name: str


@attrs.define
class AlreadyStarted(Exception):
    name: str


class State:
    def __init__(self) -> None:
        self.commands = application.Commands()
        self.commands.add_command("view_computation", ViewComputation(self))
        self.commands.add_command("start_computation", StartComputation(self))

        self.available_computations: dict[str, Computation] = {}

    def start_computation(self, name: str) -> None:
        if name in self.available_computations:
            raise AlreadyStarted(name=name)
        self.available_computations[name] = Computation(name=name)


class ViewComputation(application.Command):
    def __init__(self, state: State) -> None:
        self.state = state

    def completion(self, command: str, rest: str) -> list[str]:
        if " " in rest.strip():
            return []

        if not self.state.available_computations:
            return ["<use start_computation first>"]
        return sorted(self.state.available_computations)


class StartComputation(application.Command):
    def __init__(self, state: State) -> None:
        self.state = state

    def completion(self, command: str, rest: str) -> list[str]:
        if " " in rest.strip():
            return []
        return ["<name>"]

    def run(self, runner: application.Runner, command: str, rest: str) -> None:
        if rest:
            name = rest.split(" ", 1)[0]
            self.state.start_computation(name)
        super().run(runner, command, rest)
