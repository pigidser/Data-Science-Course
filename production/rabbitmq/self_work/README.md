The project is attended to calculate RMSE metric while acquiring sample row from a dataset (sklearn.diabets) through RabbitMQ message broker.

Necessary steps:
1. Install docker and run RabbitMQ image:
$ docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

2. Training the model and save to a file:
$ python train_model.py

3. Run service 1 which sends samples to a queue:
$ python features.py

4. Run service 2 which reads messages from the queue and calculates RMSE metric:
$ python model.py