## Requirements
- Scala 2.12 
- JDK 8
- [Apache Spark](https://spark.apache.org/) (for distributed computing) version 3.5.1


## Installation

1. **Install and setup JAVA**:

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
2.**Set up Apache Spark version 3.5.1**:

    Follow the [official guide](https://spark.apache.org/docs/latest/index.html) to set up Apache Spark in your environment.

3. **Install SBT**
   
If you haven’t installed SBT, you can do so by following instructions at the https://www.scala-sbt.org/download/

4. **Clone the repository**

    ```bash
    git clone https://github.com/Ylenia1211/Bioft.git
    cd Bioft
    ```

5. **Compile the Project**
   
In project directory there is a *build.sbt* file, open a terminal and run:

```bash
sbt compile
```

6. **Create the JAR File**
To package your project as a JAR file, run:

```bash
sbt package
```

This will create a JAR file in the target/scala-2.12/ directory, named **bioft.jar**.



