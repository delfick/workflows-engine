import typer

from . import application, state

app = typer.Typer()


@app.command()
def ui() -> None:
    application.Runner.run(state.State().commands)


def main() -> None:
    app()
