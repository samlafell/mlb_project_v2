# MLB Project

## Introduction

MLB Project is currently in the testing phase with active development being carried out in a Docker Dev Container. The output and functioning of the services are verified using the `docker-compose up` command. The setup encapsulates the application and its dependencies into a Docker container to ensure consistency across different environments, making the development process smoother and more manageable.

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

## Contribution
The project is open for contributions. Feel free to submit issues, or pull requests if you find bugs or have suggestions for improvements.

## Contact
For any inquiries or further discussion regarding the project, please contact datasolutions@samlafell.com

## License
This project is licensed under the MIT License. See the LICENSE file for details.