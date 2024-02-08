## Prerequisites
### Python
The project was developed using Python 3.11 but it may also work with other versions not older than Python 3.8.
Python should be available in the enviroment path.
### Database
New table needs to be created in the database to store script results.

### Installation
* Execute sql/ddl/snapshot_changes_ai.sql in the database (creates a new table, existing objects are not affected)
* Execute install.ps1, this script will install all necessary Python libraries

### Execution
The script to run is src/mail.py, it accepts 3 positional arguments:
* Database connection string
* OpenAI API Key
* Snapshot Id

### Example
& "venv/Scripts/python" src/main.py "host=localhost port=5432 dbname=harvester" "sk-5jfudirurrjkmiwpwptgkm" 1118

### Logging
During execution the script outputs execution logs to console, in addition it writes logs to /log/ folder to files split by hour.