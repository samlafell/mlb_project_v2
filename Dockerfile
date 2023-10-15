# Use the specified image as the base image
FROM python:3.10.13-bookworm

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install the required Python packages
ENV PATH="/root/.local/bin:${PATH}"
RUN pip3 install --upgrade pip && pip3 install --user -r requirements.txt

# This Dockerfile sets up a containerized environment for running a Python application. It installs the necessary packages and sets the working directory to /app. The application code is copied into the container and can be run from there.

# What would I run as the docker build command to build this container?
# docker build -t myapp .