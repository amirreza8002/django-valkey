import os
import shutil

from invoke import task


@task
def devenv(c):
    clean(c)
    cmd = "docker compose --profile all up -d"
    c.run(cmd)


@task
def clean(c):
    if os.path.isdir("build"):
        shutil.rmtree("build")
    if os.path.isdir("dist"):
        shutil.rmtree("dist")

    c.run("docker compose --profile all rm -s -f")
