#!/bin/bash

. venv/bin/activate

export FLASK_APP=app.py
export FLASK_ENV=development
flask run -h 0.0.0.0 -p 13387 --no-reload
