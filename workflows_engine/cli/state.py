import attrs

from . import application


@attrs.define
class Workflow:
    name: str


@attrs.define
class AlreadyStarted(Exception):
    name: str


class State:
    def __init__(self) -> None:
        self.commands = application.Commands()
        self.commands.add_command("view_workflow", ViewWorkflow(self))
        self.commands.add_command("start_workflow", StartWorkflow(self))

        self.available_workflows: dict[str, Workflow] = {}

    def start_workflow(self, name: str) -> None:
        if name in self.available_workflows:
            raise AlreadyStarted(name=name)
        self.available_workflows[name] = Workflow(name=name)


class ViewWorkflow(application.Command):
    def __init__(self, state: State) -> None:
        self.state = state

    def completion(self, command: str, rest: str) -> list[str]:
        if " " in rest.strip():
            return []

        if not self.state.available_workflows:
            return ["<use start_workflow first>"]
        return sorted(self.state.available_workflows)


class StartWorkflow(application.Command):
    def __init__(self, state: State) -> None:
        self.state = state

    def completion(self, command: str, rest: str) -> list[str]:
        if " " in rest.strip():
            return []
        return ["<name>"]

    def run(self, runner: application.Runner, command: str, rest: str) -> None:
        if rest:
            name = rest.split(" ", 1)[0]
            self.state.start_workflow(name)
        super().run(runner, command, rest)
