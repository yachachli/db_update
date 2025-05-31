import typing as t

import nox


def nox_session(
    **kwargs: t.Any,
) -> t.Callable[[t.Callable[[nox.Session], None]], t.Callable[[nox.Session], None]]:
    kwargs.setdefault("venv_backend", "uv")
    kwargs.setdefault("reuse_venv", True)

    def inner(func: t.Callable[[nox.Session], None]) -> t.Callable[[nox.Session], None]:
        return nox.session(**kwargs)(func)

    return inner


@nox_session()
def lint(session: nox.Session) -> None:
    _ = session.run_install(
        "uv",
        "sync",
        "--group=lint",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    _ = session.run("python", "-m", "ruff", "check", "--fix", ".")


@nox_session()
def typecheck(session: nox.Session) -> None:
    _ = session.run_install(
        "uv",
        "sync",
        "--group=typecheck",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    _ = session.run("python", "-m", "pyright", ".")


@nox_session()
def format(session: nox.Session) -> None:
    _ = session.run_install(
        "uv",
        "sync",
        "--group=format",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    _ = session.run(
        "python",
        "-m",
        "ruff",
        "check",
        "--select",
        "I",
        "--fix",
        ".",
    )
    _ = session.run("python", "-m", "ruff", "format", ".")
