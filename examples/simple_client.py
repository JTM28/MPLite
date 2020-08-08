from uuid import uuid4
from mplite import publish



EXAMPLE_TASK = {"task": "sleep(3)"}


for _ in range(100):
    msg = EXAMPLE_TASK
    msg['id'] = str(uuid4())
    publish(msg)