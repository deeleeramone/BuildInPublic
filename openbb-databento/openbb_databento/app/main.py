from pathlib import Path
from fastapi import FastAPI


def app() -> FastAPI:
    """Main function to run the FastAPI app."""
    import os
    from openbb_platform_api.main import app
    from openbb_core.env import Env
    from openbb_core.app.model.command_context import CommandContext
    from openbb_databento.app.options import router as options_router
    from openbb_databento.app.term_structures import router as term_structures_router
    from openbb_databento.app.udf import router as udf_router
    from openbb_databento.app.ws import create_databento_manager

    ctx = CommandContext()

    # Create a Databento manager
    api_key = getattr(ctx.user_settings.credentials, "databento_api_key", None)

    if api_key is None:
        api_key = getattr(Env(), "DATABENTO_API_KEY", None) or os.getenv(
            "DATABENTO_API_KEY", None
        )

    if api_key is None:
        raise ValueError("API key is required for Databento.")

    manager = create_databento_manager(api_key)
    app.include_router(udf_router, tags=["UDF"])
    app.include_router(manager.router, tags=["Live"])
    app.include_router(options_router, tags=["Options"])
    app.include_router(term_structures_router, tags=["Term Structures"])

    @app.get("/about")
    async def about():
        """About endpoint."""
        about_text = ""
        about_path = Path(__file__).parent.parent / "data" / "README.md"

        with open(about_path, encoding="utf-8") as f:
            about_text = f.read()

        return about_text

    return app


def main():
    """Main function to run the FastAPI app."""
    import subprocess
    import sys

    config_file_path = __file__.replace("/app/main.py", "")

    try:
        subprocess.run(
            [
                "openbb-api",
                "--app",
                __file__,
                "--name",
                "app",
                "--factory",
                "--widgets-path",
                config_file_path,
                "--no-build",
                "--apps-json",
                config_file_path,
                "--host",
                "0.0.0.0",
                "--port",
                "6940",
            ]
        )
    except KeyboardInterrupt:
        sys.exit(0)
