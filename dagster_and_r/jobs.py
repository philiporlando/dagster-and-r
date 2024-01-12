from dagster import job
from . ops import first_op, second_op


@job
def docker_container_op_r():
    second_op(first_op())