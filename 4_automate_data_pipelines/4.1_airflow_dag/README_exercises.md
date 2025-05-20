# If you're working on Udacity's integrated VSCode platform, do this after each exercise

1. Run the following two commands in the Integrated Terminal in Visual Studio Code: 
```
/opt/airflow/start-services.sh
```
```
/opt/airflow/start.sh
```

2. Create an admin user to the Airflow: 
```
airflow users create --email student@example.com --firstname aStudent --lastname aStudent --password admin --role Admin --username admin
```


3. Steps 1 and 2 are required before every Airflow session, and the integrated terminal may not allow pasting commands into. 
To save time: 

3.1. Create a shell script, name it something like start_airflow.sh and paste this into it:
```
#!/bin/bash

/opt/airflow/start-services.sh
/opt/airflow/start.sh
airflow users create --email student@example.com --firstname aStudent --lastname aStudent --password admin --role Admin --username admin
airflow scheduler
```

3.2. Upload it in the working directory, like /opt/airflow

3.3. cd into the directory where start_airflow.sh is:
```
cd /opt/airflow
```

3.4. Make it executable. In the terminal (you'll need to type this one manually once):
```
chmod +x start_airflow.sh
```

3.5. Run it with:
```
./start_airflow.sh
```


4. Open the Airflow UI using the "Access Airflow" button located on Udacity's Workspace.
5. In the Airflow UI, turn off / pause any previous exercise, turn on / toggle on the currrent exercise. 
6. Wait a moment, refresh the UI to see Airflow automatically run your DAG. 
