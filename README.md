# zitified-zephflow

`zitified-zephflow` provides an example of zitifying the zephflow sdk to listen on a ziti service for incoming data 
 to be transformed and then opens a zitified postgreSQL connection via stdoutSink.

## Build the Example
On the linux system that will run the Zephlow sdk
clone the repo
mkdir repos
cd repos
```git clone https://github.com/r-caamano/zitified-zephflow.git```
cd zitified-zephflow
```./gradlew --refresh-dependencies clean build ```

## Setup and Configure the Example

![Diagram](network.png)

1. Create or use an existing ziti network with at least one edge router. This can be accomplished using the Netfoundry
   Console.


1. Create and enroll three ziti identities
   ```
   1. FLEAK_CUST01
   2. FLEAK_ZEPHFLOW
   3. POSTGRESQL_TUN
   ```
The following assumes naming in the network diagram but can be substituted per preference:

1. Create a simple sdk service named "FLEAK_TEST" and use 192.168.55.1 and 8000 as the
   port. Assign @FLEAK_ZEPHFLOW as the hosting entity
   ![Diagram](inbound.png)
2. Create a Service Policy to enable FEAK_CUST01_TUN to access the FLEAK_TEST service
   ![Diagram](inbound_policy.png)
3. Create an advanced service named ZITIFIED_POSTGRESQL.  Set the protocol to "tcp", set the intercept ip to "zitified-pg",
   set the port to 5432, select identity @POSTGRESQL_TUN, set forward address to no and address to 127.0.0.1 and set forward port 
   to yes.
   ![Diagram](outbound.png)
4. Create a Service Policy to enable FEAK_ZEPHFLOW to access the ZITIFIED_POSTGRESQL service
   ![Diagram](outbound_policy.png)

5. Install and setup postgreSQL server on vm and install and run zeti-edge-tunnel with POSTGRESQL_TUN Identity
6. Install ziti-edge-tunnel on PostgreSQL server Install OpenZiti tunneller on client device and add identity FLEAK_CUST01 via jwt [Linux](https://netfoundry.io/docs/openziti/reference/tunnelers/linux/)
7. Create db: mydb, user: myuser, password: mypassword
8. create table transform e.g. 
   ```CREATE TABLE transform (
       id SERIAL PRIMARY KEY,          -- auto‑incrementing unique identifier
       doubled_value   INTEGER NOT NULL,
       original_value  INTEGER NOT NULL,
       status          VARCHAR(50) NOT NULL,
       timestamp       TIMESTAMP NOT NULL
       );
   ```
8. Install OpenZiti tunneller on client device and add identity FLEAK_CUST01 via jwt [Linux](https://netfoundry.io/docs/openziti/reference/tunnelers/linux/) ,[Windows](https://netfoundry.io/docs/openziti/reference/tunnelers/windows/)
9. Install netcat on client device 
10. On device hosting Zephflow sdk/ziti sdk run the program from project root enter. ```./gradlew run <ziti identity json> <inbound ziti service name> <outbound ziti url>```
    e.g. ```./gradlew run /tmp/fleak.json FLEAK_TEST zitified-pg```
11. From client enter ```echo '[{"value":150,"timestamp":1699999999}]' | ncat 192.168.55.1 8000```
