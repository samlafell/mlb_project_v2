# MLB Project

## Introduction

MLB Project is currently in the testing phase with active development being carried out in a Docker Dev Container. The output and functioning of the services are verified using the `docker-compose up` command. The setup encapsulates the application and its dependencies into a Docker container to ensure consistency across different environments, making the development process smoother and more manageable.

## Updates
### 10/29/2023:
- Enabled Schema overwrite since this is in early stages.
- Enabled the command line program src/extract_and_load.py to be a multi-year pull, will fix later to allow either single or multi-year but for now forcing multi year, could be 2018-2018 though and just pull that one year
- Overwrote Date column in delta to be an actual date column with years (before it was "Friday, Jul 17" for example.. and now its 7/17/2020), easier to parse later and actually reduced table sizes by 38%, so a very big improvement if thinking on a large scale
- Optimized a lookup from O(n) to O(1) for backup team codes
  
### 10/15/2023:
- Added in Delta Lake standalone
    - partitioned by team
    - contains 2023 game-level data for Teams
- Added [Azure Storage Info](#azurestorage) (user just needs to add storage container key in an env)

## Future Integrations

In the subsequent stages of development, the project is slated to integrate with several robust technologies including:

- **dbt Core**: For transforming and modeling data effectively in the data warehouse.
- **Delta Lake Standalone**: To provide reliable data lakes and ensure high-quality data.
- **Apache Airflow**: For orchestrating complex data workflows and scheduling jobs.

## Automation and Monitoring

The ultimate objective is to automate the data verification process such that every day, at a predefined time, a service from Azure checks the source data against the target data. In case the source data is found to be newer, the service will trigger the entire workflow to execute upsert/insert/merge operations while ensuring idempotency. 

Azure Logic Apps, a cloud service that helps schedule, automate, and orchestrate tasks, could be employed to achieve this automation. It can be set up to manage the routine tasks and handle the workflow described above, triggering the necessary actions based on the schedule and conditions defined.

## Development Environment

To set up the development environment, follow the instructions below:

1. Build the Docker image:
   ```bash
   docker build -t mlb_project_v2 .
   ``` 

2. Verify the output:
    ```bash
    docker-compose up
    ```

These commands encapsulate the application and its dependencies into a Docker container, ensuring that the application runs consistently across different environments.

As of 10/29/2023, the docker-compose up command will actually start pulling all years 2015-2023 into the environment that you've set (read below). It will populate with game-level stats for each team, coming from: http://www.baseball-reference.com/teams/{team}/{year}-schedule-scores.shtml


<a id="azurestorage"></a>
#### Set Up Azure Storage Account

Create an Azure Storage Account and find your storage account key. You will use this in the next step.

#### Configure Environment Variables:

Create a .env file in the project root and add your Azure storage account key:

**.env file**:
```
AZURE_STORAGE_ACCOUNT_KEY=your_actual_storage_account_key_here
```
Note: Never commit your .env file to the repository. It is ignored by default in the .gitignore.


## Contribution
The project is open for contributions. Feel free to submit issues, or pull requests if you find bugs or have suggestions for improvements.

## Contact
For any inquiries or further discussion regarding the project, please contact datasolutions@samlafell.com

## License
This project is licensed under the MIT License. See the LICENSE file for details.
