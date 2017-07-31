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

##### For detailed informationa about the API, please generate the swagger API documentation.
##### Follow the below steps to generate the documentation.
First run
```
mvn clean compile swagger:generate
```
Then run
```
mvn clean compile swagger2markup:convertSwagger2markup
```
The documentation for the Agent and Controller APIs will be located at these directories, respectively:  
- [`project-root/presto-agent/target/swagger/asciidoc/paths.adoc`]
- [`project-root/presto-controller/target/swagger/asciidoc/paths.adoc`]

[`project-root/presto-agent/target/swagger/asciidoc/paths.adoc`]: project-root/presto-agent/target/swagger/asciidoc/paths.adoc
[`project-root/presto-controller/target/swagger/asciidoc/paths.adoc`]: project-root/presto-controller/target/swagger/asciidoc/paths.adoc

## Configuration

Each Presto Manager process requires two configuration files:
the main configuration file, and a log levels file.

By default, the controller and agent will look to `etc/controller.properties`
and `etc/agent.properties` for their main configuration files, respectively.
To override this behavior, set the system property `config` to the file you
would like to use.

For convenience, sample configurations for the Agent and Controller are 
provided in the git repository under `etc/`. The sample files also contain
descriptions for each property.

## License

This project is licensed under the Apache License.
See the [LICENSE](LICENSE) file for details


[OpenJDK]: http://openjdk.java.net/install
[Oracle Java]: http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html