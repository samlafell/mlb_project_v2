# Use Ubuntu 20.04 as the base image
FROM ubuntu:20.04

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install Python and pip
RUN apt-get update && apt-get install -y \
    python3-pip

# Install the required Python packages
RUN pip3 install -r requirements.txt

# This Dockerfile sets up a containerized environment for running a Python application. It installs the necessary packages and sets the working directory to /app. The application code is copied into the container and can be run from there.

# What would I run as the docker build command to build this container?
# docker build -t myapp .