"""Main entry point for the gentroutils package."""

from otter import Runner


def main() -> None:
    """Main function to start the gentroutils otter runner."""
    runner = Runner("gentroutils")
    runner.start()
    runner.register_tasks("gentroutils.tasks")
    runner.run()
