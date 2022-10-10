# Databricks notebook source

import workflows

from brickflow.engine.project import Project
from brickflow.engine.task import PypiTaskLibrary

if __name__ == "__main__":

    with Project(
        "sriworkflowstest",
        git_repo="https://github.com/stikkireddy/brickflow-examples",
        provider="github",
        libraries=[PypiTaskLibrary(
            package="git+https://github.com/stikkireddy/brickflow.git"
        )]
    ) as f:
        f.add_pkg(workflows)