## DDoS Detector
A proof of concept that detects DDoS attacks in Apache log files. The log files are published to a kafka cluster
where they are later processed to detect IP addresses of bots participating in the attack.


## Local Setup (Linux)
### Prerequisites
- Maven 3
- Git 
- Java 1.8+

1. Clone this repo:
    ```bash
    git clone git@github.com:paladin2350/ddos-detector
    ```
1. Download kafka: 
    ```bash
    wget http://apache.cs.utah.edu/kafka/2.3.0/kafka_2.12-2.3.0.tgz
    ```
2. Extract kafka:
    ```bash
    tar -C $HOME -xzf kafka_2.11-2.3.0.tgz
    ```
3. Change directory to the where you cloned the repo, then start kafka:
    ```bash
    ./start-kafka.sh
    ```
4. Run the detector:
    ```bash
    ./run.sh
    ```

