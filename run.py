#!/usr/bin/python3

import argparse
import os
import glob

parser = argparse.ArgumentParser(
    prog="run.py",
    description="setup airflow project"
)

parser.add_argument('-e', "--environment", action="store_true")
parser.add_argument('-b', "--build", action="store_true")

args = parser.parse_args()

if args.environment:
    os.system("docker compose up -d")

if args.build:
    for dockerFile in glob.iglob("./**/Dockerfile", recursive=True):
        path_splited = dockerFile.split('/')
        image_name = path_splited[len(path_splited) - 2]
        os.system(f"docker build {path_splited[0]}/{path_splited[1]}/{path_splited[2]} -t {image_name}")