# Exercise 4 - Is there data?

- Create a connection to `https://httpstat.us/200?sleep=20000` where `20000` indicates the delay before a response is returned
- Create a DAG that will check the connection and run a `do_something` task when a response is received 
