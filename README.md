## Installation

1. Install and setup JAVA:

    To install Liberica JDK 1.8.0_422 on your system, you can follow the instructions provided in the official Liberica documentation https://bell-sw.com/pages/downloads/#jdk-8-lts. Once the installation is complete, make sure to configure your development environment to point to the new Java installation.

```bash
        /usr/libexec/java_home -V
        export JAVA_HOME=`/usr/libexec/java_home -v 1.8.0_422-b06`
        nano ~/.zshrc
        export JAVA_HOME=$(/usr/libexec/java_home -v 1.8.0_422-b06)
```
Press CTRL+X to exit the editor Press Y to save your changes and check:
        
```bash        
        source ~/.zshrc
        echo $JAVA_HOME
        java -version
 
```
2. Set up Apache Spark version 3.5.1:

    Follow the [official guide](https://spark.apache.org/docs/latest/index.html) to set up Apache Spark in your environment.
