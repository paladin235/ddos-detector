## DDoS Detector
A proof of concept that detects DDoS attacks in Apache log files. The log files are published to a kafka cluster
where they are later processed to detect IP addresses of bots participating in the attack.


## Local Setup (Linux)
### Prerequisites
- Java 1.8+
- Maven 3
- Git 

1. Clone this repo:
    ```bash
    git clone git@github.com:paladin235/ddos-detector
    ```
1. Download kafka: 
    ```bash
    wget http://apache.cs.utah.edu/kafka/2.3.0/kafka_2.12-2.3.0.tgz
    ```
2. Extract kafka:
    ```bash
    tar -C $HOME -xzf kafka_2.12-2.3.0.tgz
    ```
3. Change directory to where you cloned the repo, then start kafka:
    ```bash
    ./start-kafka.sh
    ```
4. Run the detector:
    ```bash
    ./run.sh
    ```

