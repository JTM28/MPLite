# MPLite Documentation
In Development

## Overview
MPLite is a pure python distributed message queue system that was developed due to python's lack of a priority queue 
that could be shared across multiple cores (processes). 

MPLite can easily be run interally within a LAN cluster communicating over the localhost address, 
but since message passing is done across sockets also supports remote message passing if desired.


#### Registering a Handler



## Technical Concepts 

#### Overview
MPLite operates by using 2 instances of the multiprocessing.Manager.dict object. One of these instances,
the shared_index, containing a key value pair using a unique message id (a randomly generated uuid) as 
the key and the value being the corresponding priority, and a shared_queue dict instance which contains
the same uuid key with the value being the payload of the message. To process the highest priorty 
messages first, a sorting algorithm is run on the shared index, resulting in a sorted shared_index of
descending order by highest value priority to lowest value priority. The top key value pair of the 
shared index is then popped by the next available consumer, matching this uuid key to its respective
uuid key in the shared_queue instance.

#### Avoiding Duplicate Consumers Processing the Same Message
To avoid multiple consumers redundantly processing the same message, a single instance of the
multiprocessing.Lock object is passed to all the consumers. When 1 or more consumers are not currently
processing a message, they will continuously loop until the shared queue is free and then one 
consumer will subsequently acquire the lock, retrieve the next message and delete it from both the 
shared index and shared queue, and then release the lock before moving on to processing the message.


#### Handling Multiple Messages With the Same Priority
For messages with the same value of priority waiting to be processed. The next sorting operation
is run on a unix timestamp (to the microsecond) of when the message was posted. Essentially, messages
with the same priority value are next processed by lowest timestamp up.





 