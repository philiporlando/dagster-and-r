import dagster as dg
from . ops import first_op, second_op


@dg.job
def docker_container_op_r():
    second_op(first_op())