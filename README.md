# Goals and non-Goals
Goals:
- Demonstrate RabbitMQ can be thought of as a streaming platform.  

Non-goals:
- Demonstrate other messaging concerns like ordering, message delivery guarantees, etc.

# First application - reactive-amqp-importer 

```
cd reactive-amqp-importer
mvn install
mvn spring-boot:run
```

## Developer guide

# Run single node RabbitMQ node
```
docker run -d --name rabbitmq -p 15672:15672 -p 5672:5672 rabbitmq:3.7-management
```

> for the future we will use docker-compose :
> ```
> docker-compose up
> ```

