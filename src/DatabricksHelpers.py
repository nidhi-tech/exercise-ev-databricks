# Databricks notebook source
pip install wget

# COMMAND ----------

import os
import wget
import sys
import shutil
import zipfile

sys.stdout.fileno = lambda: False  # prevents AttributeError: 'ConsoleBuffer' object has no attribute 'fileno'


class DataDerpDatabricksHelpers:
    def __init__(self, dbutils, exercise_name):
        self.dbutils = dbutils
        self.exercise_name = exercise_name

    def current_working_user(self) -> str:
        return self.dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

    def current_user(self) -> str:
        return self.dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split("@")[0]

    def working_directory(self) -> str:
        return f"/FileStore/{self.current_user()}/{self.exercise_name}"

    def tmp_working_directory(self) -> str:
        return f"{os.getcwd()}/{self.current_user()}/tmp"

    def clean_working_directory(self):
        print(f"Cleaning up/removing files in {self.working_directory()}")
        self.dbutils.fs.rm(self.working_directory(), True)

    def clean_user_directory(self):
        dir = f"/FileStore/{self.current_user()}"
        print(f"Cleaning up/removing files in {dir}")
        self.dbutils.fs.rm(dir, True)

    def clean_remake_dir(self):
        if os.path.isdir(self.tmp_working_directory()): shutil.rmtree(self.tmp_working_directory())
        os.makedirs(self.tmp_working_directory())

    def download_to_local_dir(self, url):
        filename_parser = lambda y: y.split("/")[-1].replace("?raw=true","")
        self.clean_remake_dir()
        filename = (filename_parser)(url)
        tmp_path = f"{self.tmp_working_directory()}/{filename}"
        target_path = f"{self.working_directory()}/{filename}"
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

        saved_filename = wget.download(url, out=tmp_path)
        print(f"Downloaded tmp file: {saved_filename}")

        if tmp_path.endswith(".zip"):
            print(f"Extracting: {tmp_path}")
            with zipfile.ZipFile(tmp_path, 'r') as zip_ref:
                zip_ref.extractall(self.tmp_working_directory())

        self.dbutils.fs.cp(f"file:{self.tmp_working_directory()}/", self.working_directory(), True)
        print(f"Successfully copied to {target_path}")
        return target_path

    def stop_all_streams(self, spark):
        print("Stopping all streams")
        for s in spark.streams.active:
            try:
                s.stop()
            except:
                pass
        print("Stopped all streams")
        return

# COMMAND ----------


