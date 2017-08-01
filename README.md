# Presto Manager

A tool for managing Presto clusters.

## Getting Started

These instructions will get you an instance of the project up and running
on your local machine for development and testing purposes.

### Prerequisites

- Linux OS
- Java Development Kit 8, available at [oracle.com][Oracle Java] or [openjdk.java.net][OpenJDK]
- Python 2.4+, available at [python.org](https://www.python.org/downloads/)

### Installing

Clone the full distribution
```
git clone https://github.com/prestodb/presto-manager.git
```

## Compiling and Running

Change to the project directory you cloned the distribution
```
cd presto-manager
```

Use Maven  Wrapper to compile and generate executable JAR for the project
```
./mvnw clean install
```

### Executing

These commands will start the Agent and Controller, respectively:
```
java -jar ./presto-manager-agent/target/AgentServer.jar
java -jar ./presto-manager-controller/target/ControllerServer.jar
```

## Usage

Any Http client will work as long as it can send basic Http requests
(i.e GET, PUT, POST, DELETE, etc.)  
Postman, a Google chrome plugin, is recommended for its friendly user interface.

For example, this request will receive a list of available configuration files as its response.
```
GET http://localhost:8080/v1/config?scope=cluster
```

Please refer to the swagger API documentation for more information.

## License

This project is licensed under the Apache License.
See the [LICENSE](LICENSE) file for details


[OpenJDK]: http://openjdk.java.net/install
[Oracle Java]: http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html