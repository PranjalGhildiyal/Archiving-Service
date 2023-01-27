from archiving import Archiving, archive
from flask import Flask, render_template, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask,request, render_template
from flask_apscheduler import APScheduler
import logging as lg
from .Config.index import read_config_file

configs = read_config_file('scheduler')
minutes = float(configs['job_repeat_time'])
args = configs['instructions']
args = args.split(',')

app = Flask(__name__)
sched = APScheduler()

if __name__ == "__main__":
    sched.add_job(id = 'Archiving', func=archive, trigger="interval", minutes=minutes)
