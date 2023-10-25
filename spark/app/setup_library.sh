#!/bin/bash
export PROJECT_ID="int-demo-looker"

gsutil cp gs://int-demo-looker-spark/scripts/requirements.txt .
pip3 install -r requirements.txt
