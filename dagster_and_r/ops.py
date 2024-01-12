from dagster_docker import docker_container_op

# TODO DRY this out later? Use resources?
image = "philiporlando/test-dagster-and-r:latest"
cmd = ["Rscript", "hello_world.R"]

first_op = docker_container_op.configured(
    {
        "image": image,
        "command": cmd,
    },
    name="first_op",
)


second_op = docker_container_op.configured(
    {
        "image": image,
        "command": cmd,
    },
    name="second_op",
)