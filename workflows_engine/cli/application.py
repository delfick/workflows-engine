import typing as tp
from collections.abc import Iterable
from functools import partial

from prompt_toolkit import Application
from prompt_toolkit.application.current import get_app
from prompt_toolkit.buffer import Buffer
from prompt_toolkit.completion import CompleteEvent, Completion, WordCompleter
from prompt_toolkit.document import Document
from prompt_toolkit.filters import Condition
from prompt_toolkit.key_binding import KeyBindings, KeyPressEvent
from prompt_toolkit.key_binding.bindings.focus import focus_next, focus_previous
from prompt_toolkit.layout.containers import (
    Float,
    FloatContainer,
    HSplit,
    VSplit,
    Window,
    WindowAlign,
)
from prompt_toolkit.layout.controls import BufferControl, DummyControl
from prompt_toolkit.layout.layout import Layout
from prompt_toolkit.layout.menus import CompletionsMenu
from prompt_toolkit.widgets import SearchToolbar, TextArea


class WithText(tp.Protocol):
    text: str


class Command:
    def completion(self, command: str, rest: str) -> list[str]:
        return []

    def run(self, runner: "Runner", command: str, rest: str) -> None:
        runner.set_right_text(f"Ran {command} {rest}")


class Commands:
    def __init__(self) -> None:
        self.commands: dict[str, Command] = {}

    def add_command(self, name: str, command: Command) -> None:
        self.commands[name] = command

    def completion(self, buffer: WithText) -> list[str]:
        if " " in buffer.text:
            command, rest = buffer.text.split(" ", 1)
        else:
            command = buffer.text
            rest = ""

        if command not in self.commands and not rest:
            return sorted(self.commands)
        elif command in self.commands:
            return self.commands[command].completion(command, rest)
        else:
            return []

    def run(self, runner: "Runner", buffer: WithText) -> None:
        if " " in buffer.text:
            command, rest = buffer.text.split(" ", 1)
        else:
            command = buffer.text
            rest = ""

        if command in self.commands:
            try:
                self.commands[command].run(runner, command, rest)
            except Exception as e:
                error = f"Error: {type(e).__name__}: {str(e)}"
            else:
                return
        else:
            error = f"{command} is not a valid command"

        runner.set_right_text(error)
        return


class ExtraWordCompleter(WordCompleter):
    def get_completions(
        self, document: Document, complete_event: CompleteEvent
    ) -> Iterable[Completion]:
        completions = list(super().get_completions(document, complete_event))
        yield from completions

        if not completions:
            words = self.words
            if callable(words):
                words = words()

            word_before_cursor = document.get_word_before_cursor(
                WORD=self.WORD, pattern=self.pattern
            )

            for suggestion in words:
                yield Completion(
                    text=suggestion,
                    start_position=-len(word_before_cursor),
                    display=suggestion,
                    display_meta="",
                )


class Runner:
    left_window: Window
    right_window: Window

    @classmethod
    def run(cls, commands: Commands) -> None:
        instance = cls(commands)
        app: Application = Application(
            layout=Layout(instance.root_container),
            key_bindings=instance.make_keybindings(),
            full_screen=True,
        )
        app.run()

    def __init__(self, commands: Commands) -> None:
        self.commands = commands

        self.left_window = Window(DummyControl())
        self.right_window = Window(DummyControl())

        self.search_field = SearchToolbar()

        self.command_area = TextArea(
            height=1,
            prompt=">>> ",
            style="class:input-field",
            multiline=False,
            wrap_lines=False,
            search_field=self.search_field,
            accept_handler=self.run_prompt,
        )
        self.command_area.completer = ExtraWordCompleter(
            partial(self.commands.completion, self.command_area)
        )

        self.main_body = VSplit(
            [
                self.left_window,
                Window(width=1, char="\u2502", style="class:line"),
                self.right_window,
            ]
        )

        self.status_area = Window(height=1, content=DummyControl(), align=WindowAlign.LEFT)

        float_content = HSplit(
            [
                HSplit(
                    [
                        self.command_area,
                        self.search_field,
                        Window(height=1, char="\u2500", style="class:line"),
                    ]
                ),
                self.main_body,
                self.status_area,
            ]
        )

        self.root_container = FloatContainer(
            content=float_content,
            floats=[
                Float(
                    xcursor=True,
                    ycursor=True,
                    content=CompletionsMenu(max_height=16, scroll_offset=1),
                )
            ],
        )

    def make_keybindings(self) -> KeyBindings:

        kb = KeyBindings()

        kb.add("c-n")(focus_next)
        kb.add("c-p")(focus_previous)

        @kb.add("c-c", eager=True)
        @kb.add("c-q", eager=True)
        def _(event: KeyPressEvent):
            """
            Pressing Ctrl-Q or Ctrl-C will exit the user interface.

            Note that Ctrl-Q does not work on all terminals. Sometimes it requires
            executing `stty -ixon`.
            """
            event.app.exit()

        def not_in_command_area() -> bool:
            app = get_app()
            return app.layout.current_control != self.command_area

        @kb.add("o", filter=Condition(not_in_command_area))
        def _(event: KeyPressEvent):
            app = get_app()
            app.layout.focus(self.command_area)

        return kb

    def run_prompt(self, buffer: WithText) -> bool:
        self.commands.run(self, buffer)
        # Don't keep text
        return False

    def set_right_text(self, text: str) -> None:
        buf = Buffer(read_only=True)
        buf.set_document(Document(text), bypass_readonly=True)
        self.right_window.content = BufferControl(buffer=buf)
