
from flask import Flask,redirect,request

from helpers import make_celery

application = Flask(__name__)

application.config.update(
    CELERY_BROKER_URL='sqla+sqlite:///',
    CELERY_BACKEND='sqla+sqlite:///'
)

celery = make_celery(application)

@celery.task()
def add_together(a, b):
  return a + b

@application.route('/monitor', methods=['GET'])
def status():
  return render_template('index.html')



if __name__ == '__main__':
  app.run(debug=True)