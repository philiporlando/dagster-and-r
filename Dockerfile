# This Dockerfile is used to build the philiporlando/dagster-and-r image
# that is used with the docker_container_op

# Use the rocker/r-base image as the base image
FROM rocker/r-base

# Copy the R script from your local file system to the image
COPY ./dagster_and_r/R/hello_world.R /hello_world.R

# Set the working directory to where the R script is located
WORKDIR /

# The default command to run when the container starts
# Dagster will run this command directly 
# CMD ["Rscript", "hello_world.R"] 