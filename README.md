# distributed-rollups
This repo was setup to provide an example of constructs that can be used in CRDB to create aggregated views for near-real time reporting.  For more information see our blog post, [Real-Time Analytic Queries with CRDB, In Reality](https://medium.com/@jleelong_8398/real-time-analytic-queries-with-crdb-in-reality-f1317f049fde), on Medium.


## Clone the Repository
First install [git](https://git-scm.com) if you don't already have it.  Instructions for Mac, Windows or Linux can be found [here](https://www.atlassian.com/git/tutorials/install-git).  Then open a Mac Terminal or Windows PowerShell in your workspace folder (or wherever you keep your local repositories) and execute the following command.
```
git clone https://github.com/roachlong/distributed-rollups.git
cd distributed-rollups
git status
```


## Cockroach
If we're executing the PoC as a stand alone lab we can install and run a single node instance of cockroach on our laptops.  We're using version 25.2 to take advantage of the new As Of System Time (AOST) support for materialized views.  For Mac you can install CRDB with ```brew install cockroachdb/tap/cockroach@25.2```.  Otherwise you can download and extract the latest binary from [here](https://www.cockroachlabs.com/docs/releases), then add the location of the cockroach.exe file (i.e. C:\Users\myname\AppData\Roaming\cockroach) to your Windows Path environment variable.

Then open a new Mac Terminal or PowerShell window and execute the following command to launch your single node database.
```
cockroach start-single-node --insecure --store=./data
```
Then open a browser to http://localhost:8080 to view the dashboard for your local cockroach instance


## Initial Schema
First we'll store the connection string as a variable in our terminal shell window.  On Mac variables are assigned like ```my_var="example"``` and on Windows we proceed the variable assignment with a $ symbol ```$my_var="example"```.
```
conn_str="postgresql://localhost:26257/defaultdb?sslmode=disable"
```

Then we'll execute the sql to create a sample schema and load some data into it.
```
cockroach sql --url "$conn_str" -f 00-initial-schema.sql
export conn_str="${conn_str/defaultdb/schedules}"
cockroach sql --url "$conn_str" -f 01-populate-sample-data.sql
```

And create the schema objects, indexes and functions to support our aggregated reporting solution.
```
cockroach sql --url "$conn_str" -f 02-create-flight-snapshot.sql
cockroach sql --url "$conn_str" -f 03-create-analytics-function.sql
```

Finally, before we run the workload tests we'll setup a cron job to refresh our materialized view every minute by adding a similar line below to ```crontab -e```.
```
* * * * * /opt/homebrew/bin/cockroach sql --url "postgresql://localhost:26257/schedules?sslmode=disable" -e="REFRESH MATERIALIZED VIEW flight_snapshot;"
```
And then check the status with ```grep cron /var/log/system.log``` and ```cat /var/mail/username```


## dbworkload
This is a tool we use to simulate data flowing into cockroach, developed by one of our colleagues with python.  We can install the tool with ```pip3 install "dbworkload[postgres]"```, and then add it to your path.  On Mac or Linux with Bash you can use:
```
echo -e '\nexport PATH=`python3 -m site --user-base`/bin:$PATH' >> ~/.bashrc 
source ~/.bashrc
```
For Windows you can add the location of the dbworkload.exe file (i.e. C:\Users\myname\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.9_abcdefghijk99\LocalCache\local-packages\Python39\Scripts) to your Windows Path environment variable.  The pip command above should provide the exact path to your local python executables.


## Execute Transactions
We can control the velocity and volume of the workload with a few properties described below.
* num_connections: we'll simulate the workload across a number of processes
* duration: the number of minutes for which we want to run the simulation
* schedule_freq: the percentage of cycles we want to make updates to the flight schedule
* status_freq: the percentage of cycles we want to make updates to flight status
* inventory_freq: the percentage of cycles we want to make updates to the available seating
* price_freq: the percentage of cycles we want to make updates to the ticket prices
* batch_size: the number of records we want to update in a single cycle
* delay: the number of milliseconds we should pause between transactions, so we don't overload admission controls

We'll store this information as variables in the terminal shell window. On Mac variables are assigned like ```my_var="example"``` and on Windows we proceed the variable assignment with a $ symbol ```$my_var="example"```.
```
conn_str="postgresql://root@localhost:26257/schedules?sslmode=disable&application_name=transaction_workload"
num_connections=4
duration=60
schedule_freq=10
status_freq=90
inventory_freq=75
price_freq=25
batch_size=16
delay=100
```

Then we can use our dbworkload script to simulate the workload.  **Note**: with Windows PowerShell replace each backslash double quote(\\") with a pair of double quotes around the json properties, i.e. ``` ""batch_size"": ""$batch_size"" ```
```
dbworkload run -w transactions.py -c $num_connections -d $(( ${duration} * 60 )) --uri "$conn_str" --args "{
        \"schedule_freq\": $schedule_freq,
        \"status_freq\": $status_freq,
        \"inventory_freq\": $inventory_freq,
        \"price_freq\": $price_freq,
        \"batch_size\": $batch_size,
        \"delay\": $delay
    }"
```


## Run Analytics
We'll setup a second workload to run analytics with a slight delay between each report query.
* delay: the number of milliseconds we should pause between queries, so we don't overload admission controls

We'll store this information as variables in the terminal shell window. On Mac variables are assigned like ```my_var="example"``` and on Windows we proceed the variable assignment with a $ symbol ```$my_var="example"```.
```
conn_str="postgresql://root@localhost:26257/schedules?sslmode=disable&application_name=analytics_workload"
num_connections=1
duration=60
delay=100
```

Then we can use our dbworkload script to simulate the workload.  **Note**: with Windows PowerShell replace each backslash double quote(\\") with a pair of double quotes around the json properties, i.e. ``` ""delay"": ""$delay"" ```
```
dbworkload run -w analytics.py -c $num_connections -d $(( ${duration} * 60 )) --uri "$conn_str" --args "{
        \"delay\": $delay
    }"
```

