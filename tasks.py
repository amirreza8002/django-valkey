import os
import shutil

from invoke import run, task


@task
def devenv(c):
    clean(c)
    cmd = "docker compose --profile all up -d"
    run(cmd)


@task
def clean(c):
    if os.path.isdir("build"):
        shutil.rmtree("build")
    if os.path.isdir("dist"):
        shutil.rmtree("dist")

    run("docker compose --profile all rm -s -f")
