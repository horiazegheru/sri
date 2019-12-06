# sri
sri project

install elasticsearch si Flask de aici: https://dev.to/aligoren/using-elasticsearch-with-python-and-flask-2i0e
dataset de aici: https://www.kaggle.com/wcukierski/enron-email-dataset
set populate_elastic = True

python3 main.py

GET localhost:5000/ ar trebui sa populeze elasticul
POST localhost:5000/search {"keyword": "whatever"} pt verificare