Goals:
- Demonstrate rabbitmq streaming capabilities, i.e. that we can build proper streaming applications almost the same way people use Kakfa (e.g. using Kafka Streams).

Non-goals:
- Demonstrate other messaging concerns like ordering, message delivery guarantees, etc.


How to run Rabbitmq for our development purposes:
```
docker run -d --name rabbitmq -p 15672:15672 -p 5672:5672 rabbitmq:3.7-management
```

for the future we will use docker-compose :
```
docker-compose up
```

