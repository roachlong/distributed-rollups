# distributed-rollups
providing an example of constructs that can be used in CRDB to create aggregated views for near-real time reporting

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
cockroach sql --url "$conn_str" -f 02-create-flight-snapshot.sql
```
